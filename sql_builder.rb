require 'lru_redux'

class SqlBuilder

  def initialize(template,klass=nil)
    @args = {}
    @sql = template
    @sections = {}
    @klass = klass
  end

  [:set, :where2,:where,:order_by,:limit,:left_join,:join,:offset, :select].each do |k|
    define_method k do |data, args = {}|
      @args.merge!(args)
      @sections[k] ||= []
      @sections[k] << data
      self
    end
  end

  def to_sql
    sql = @sql.dup

    @sections.each do |k,v|
      joined = nil
      case k
      when :select
        joined = "SELECT " << v.join(" , ")
      when :where, :where2
        joined = "WHERE " << v.map{|c| "(" << c << ")" }.join(" AND ")
      when :join
        joined = v.map{|item| "JOIN " << item }.join("\n")
      when :left_join
        joined = v.map{|item| "LEFT JOIN " << item }.join("\n")
      when :limit
        joined = "LIMIT " << v.last.to_s
      when :offset
        joined = "OFFSET " << v.last.to_s
      when :order_by
        joined = "ORDER BY " << v.join(" , ")
      when :set
        joined = "SET " << v.join(" , ")
      end

      sql.sub!("/*#{k}*/", joined)
    end
    sql
  end

  def exec(args = {})
    @args.merge!(args)

    sql = to_sql

    unless @args == {}
      sql = ActiveRecord::Base.send(:sanitize_sql_array, [sql, @args])
    end

    if @klass
      @klass.find_by_sql(sql)
    else
      ActiveRecord::Base.connection.execute(sql)
    end
  end

  def self.map_exec(klass, sql, args = {})
    self.new(sql).map_exec(klass, args)
  end

  class RailsDateTimeDecoder < PG::SimpleDecoder
    def decode(string, tuple=nil, field=nil)
      if Rails.version >= "4.2.0"
        @caster ||= ActiveRecord::Type::DateTime.new
        @caster.type_cast_from_database(string)
      else
        ActiveRecord::ConnectionAdapters::Column.string_to_time string
      end
    end
  end


  class ActiveRecordTypeMap < PG::BasicTypeMapForResults
    def initialize(connection)
      super(connection)
      rm_coder 0, 1114
      add_coder RailsDateTimeDecoder.new(name: "timestamp", oid: 1114, format: 0)
      # we don't need deprecations
     	self.default_type_map = PG::TypeMapInRuby.new
    end
  end

  def self.pg_type_map
    conn = ActiveRecord::Base.connection.raw_connection
    @typemap ||= ActiveRecordTypeMap.new(conn)
  end

  # deserialize row
  def make_deserializer(fields)
    setters = fields.each_with_index.map do |f,index|
      "o.#{f} = results.getvalue(row_number, #{index})"
    end.join("\n")
    eval <<CODE 
    Class.new do
      def self.deserialize(results, klass, row_number)
        o = klass.new
        #{setters}
        o
      end
    end
CODE
  end

  def get_deserializer(fields)
    @@deserializer_cache ||= LruRedux::Cache.new(200)
    @@deserializer_cache[fields] ||= make_deserializer(fields)
  end

  def map_exec(klass = OpenStruct, args = {})
    results = exec(args)
    results.type_map = SqlBuilder.pg_type_map

    deserializer = get_deserializer(results.fields)

    i = 0
    rows = []
    while i < results.num_tuples
      rows << deserializer.deserialize(results, klass, 0)
      i += 1
    end
    rows
  end

end

class ActiveRecord::Base
  def self.sql_builder(template)
    SqlBuilder.new(template, self)
  end
end