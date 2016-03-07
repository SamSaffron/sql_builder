require 'active_record'
require 'pg'
require 'benchmark/ips'
require 'sequel'

load 'sql_builder.rb'

ActiveRecord::Base.establish_connection({
  adapter: 'postgresql',
  db: 'postgresql'
})


$raw_connection = ActiveRecord::Base.connection.raw_connection

DB = Sequel.postgres($raw_connection.db, :host => 'localhost')
DB.optimize_model_load = true

DB << "DROP TABLE cars" rescue nil
DB << "CREATE TABLE cars(id serial primary key, make varchar, color varchar, max_speed int)"
DB << "CREATE INDEX idx_color ON cars (color, max_speed)"

DB << "DROP TABLE topics" rescue nil
DB << "DROP TABLE posts" rescue nil
DB << "DROP TABLE users" rescue nil
DB << "DROP TABLE categories" rescue nil

DB << "CREATE TABLE topics(id serial primary key, user_id int, title varchar, created_at timestamp without time zone, updated_at timestamp without time zone"

DB << "CREATE TABLE posts(id serial primary key, topic_id int, body varchar, created_at timestamp, updated_at timestamp without time zone)"

class Car < ActiveRecord::Base
end

def add_cars(make, color, count)
  count.times do
    Car.create!(make: make, color: color, max_speed: 200)
  end
end

def sequel(color, max_speed)
  cars = DB[:cars]
  cars = cars.where(color: color) if color
  cars = cars.where('max_speed > ?', max_speed) if max_speed
  cars = cars.select(:make, :max_speed)
  cars.each do |car|
    "make: #{car[:make]} max_speed: #{car[:max_speed]}"
  end
end

class CarSequel < Sequel::Model(:cars)
end

def sequel_obj(color, max_speed)
  cars = CarSequel.where(color: color) if color
  cars = cars.where('max_speed > ?', max_speed) if max_speed
  cars = cars.select(:make, :max_speed)
  cars.each do |car|
    "make: #{car.make} max_speed: #{car.max_speed}"
  end
end

def active_record(color, max_speed)
  cars = Car.all
  cars = cars.where(color: color) if color
  cars = cars.where('max_speed > ?', max_speed) if max_speed
  cars = cars.select('make,max_speed')
  cars.each do |car|
    "make: #{car.make} max_speed: #{car.max_speed}"
  end
end

def raw(color, max_speed)
  sql = "select make, max_speed from cars"
  and_or_where = " where "
  if color
      sql << " where color = '#{PG::Connection.escape(color)}'"
        and_or_where = " and"
  end
  sql << " #{and_or_where} max_speed > '#{max_speed}'" if max_speed
  $raw_connection.exec(sql).each do |row|
    "make: #{row["make"]} max_speed: #{row["max_speed"]}"
  end
end

# like car but without AR
class Car2
  attr_accessor :id, :make, :max_speed, :color
end

def sql_builder(color, max_speed)
  builder = SqlBuilder.new("select make, max_speed from cars /*where*/")
  builder.where("color = :color", color: color) if color
  builder.where("max_speed > :max_speed",
                               max_speed: max_speed) if max_speed
  builder.map_exec(Car2).each do |row|
    "make: #{row.make} max_speed: #{row.max_speed}"
  end
end

cars = [[1,"red"],[100, "blue"],[1000,"green"]]

cars.each do |count, color|
  add_cars("ford", color, count)
end


Benchmark.ips do |b|
  cars.each do |count, color|

    b.report("#{count} row raw") do
      raw color, 100
    end

    b.report("#{count} row sequel hash") do
      sequel color, 100
    end

    b.report("#{count} row sequel object") do
      sequel_obj color, 100
    end

    b.report("#{count} row active_record") do
      active_record color, 100
    end

    b.report("#{count} row sql_builder") do
      sql_builder color, 100
    end

  end
end


