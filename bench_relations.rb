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

DB << "CREATE TABLE topics(id serial primary key, category_id int, title varchar, created_at timestamp without time zone, updated_at timestamp without time zone)"

DB << "CREATE TABLE posts(id serial primary key, user_id int, topic_id int, post_number int, body varchar, created_at timestamp, updated_at timestamp without time zone)"

DB << "CREATE TABLE users(id serial primary key, email varchar, name varchar, password varchar, age int, created_at timestamp without time zone, updated_at timestamp without time zone)"

DB << "CREATE TABLE categories(id serial primary key, name varchar)"

class Topic < ActiveRecord::Base
  has_many :posts
  belongs_to :category
end

class TopicSequel < Sequel::Model(:topics)
  one_to_many :posts, :class => :PostSequel
  many_to_one :category, :class => :CategorySequel
end

class Post < ActiveRecord::Base
  belongs_to :user
  belongs_to :topic
end

class PostSequel < Sequel::Model(:posts)
  many_to_one :user, :class => :UserSequel
  many_to_one :topic, :class => :TopicSequel
end

class User < ActiveRecord::Base
  has_many :post
end

class UserSequel < Sequel::Model(:users)
  one_to_many :post, :class => :PostSequel
end

class Category < ActiveRecord::Base
  has_many :topic
end

class CategorySequel < Sequel::Model(:categories)
  one_to_many :topic, :class => :TopicSequel
end

def build_categories(n)
  n.times do |i|
    Category.create!(name: "category#{i}")
  end
end

def build_topics(n)
  category_ids = Category.pluck(:id)
  n.times do |i|
    Topic.create!(title: "topic title #{i}", category_id: category_ids.sample)
  end
end

def build_users(n)
  n.times do |i|
    User.create!(email: "test#{i}@email.com", name: "user name#{i}", age: i, password: "xzy123#{i}")
  end
end

def build_posts(n)
  user_ids = User.pluck(:id)
  topic_map = Topic.pluck(:id).map do |id|
    [id,0]
  end

  n.times do |i|
    topic_id, post_number = arr = topic_map.sample
    Post.create!(post_number: post_number, topic_id: topic_id, body: "Lorum ipsum #{n}" * 50, user_id: user_ids.sample)
    arr[1] = post_number+1
  end
end

build_categories(10)
build_topics(10)
build_users(10)
build_posts(300)

def active_record_topic(topic_id)
  result = ""
  Post.where(topic_id: topic_id)
      .order(:post_number)
      .includes(:user)
      .limit(20).each do |post|
        result << "#{post.topic.title} #{post.user.name} #{post.body} #{post.created_at.to_date}\n"
  end

  result
end

def sequel_topic(topic_id)
  result = ""
  PostSequel.where(topic_id: topic_id)
      .order(:post_number)
      .limit(20).each do |post|
        result << "#{post.topic.title} #{post.user.name} #{post.body} #{post.created_at.to_date}\n"
  end

  result
end

# p active_record_topic(Topic.first.id) == sequel_topic(Topic.first.id)

$topic_id = Topic.first.id

Benchmark.ips do |b|

  b.report("AR topic") do
    active_record_topic($topic_id)
  end

  b.report("Sequel topic") do
    sequel_topic($topic_id)
  end

end


