#!/usr/bin/env ruby
## Copyright Grzegorz Blaszczyk Consulting 2011

MaxRSSItems = 100

DbName = 'currency_rates'
DbTable = 'rates'

### DO NOT EDIT BELOW THIS LINE ###

require 'sqlite3'
require 'rss'

def fetch_rates()
  return RSS::Parser.parse(open('http://rss.nbp.pl/kursy/TabelaA.xml').read, false).items[0..MaxRSSItems-1]
end

def get_rate(currency_code, items)

  items.each do |item|
    plain_description = item.description.gsub(/<\/?[^>]*>/, "")
    key_value_pair = plain_description.scan(/1 #{currency_code} =[0-9,]+/)[0].gsub(/,/,".").split('=')

    rate = Hash.new
    rate['code'] = currency_code
    rate['table_no'] = item.title.split(' ')[2]
    rate['date'] = item.title.split(' ')[5]
    rate['value'] = key_value_pair[1].to_f
    return rate
  end
end

def to_foreign_currency (currency_code, amount, items)
  return amount / get_rate(currency_code, items)['value'].to_f
end

def from_foreign_currency (currency_code, amount, items)
  return amount * get_rate(currency_code, items)['value'].to_f
end

def verify_database
  ### Creating database if it does not exist ###

  database = SQLite3::Database.new("#{DbName}.db")

  database.execute( "CREATE TABLE IF NOT EXISTS #{DbTable} (
    id INTEGER PRIMARY KEY,
    code TEXT,
    table_no TEXT, 
    date_created DATE,
    value REAL
  )")

  return database
end

def save_or_update_in_database(database, rate)

  puts "Filling database..."
  select_query = "select value FROM #{DbTable} WHERE code = ? AND date_created = ? ORDER BY date_created DESC LIMIT 1"
  insert_query = "insert into #{DbTable} (id,code,table_no,date_created,value) values (null,?,?,?,?)"
  
  select_statement = database.prepare(select_query)
  insert_statement = database.prepare(insert_query)

  select_statement.bind_param(1, rate['code'])
  select_statement.bind_param(2, rate['date'])

  insert_statement.bind_param(1, rate['code'])
  insert_statement.bind_param(2, rate['table_no'])
  insert_statement.bind_param(3, rate['date'])
  insert_statement.bind_param(4, rate['value'])

  rows = select_statement.execute!(rate['code'])
  if !rows.nil? and rows.length > 0
    puts "Data up to date..."
  else
    puts "Executing first insert for #{rate['code']} today..."
    insert_statement.execute!
  end
  select_statement.close
  insert_statement.close
  
end

items = fetch_rates()

##Testing
rate = get_rate('EUR', items)
puts "Table: #{rate['table_no']}"
puts "Date: #{rate['date']}"
puts "1 EUR = #{rate['value']}"

from_foreign_currency = from_foreign_currency('EUR', 100, items)
puts "100 EUR = #{from_foreign_currency}"

to_foreign_currency = to_foreign_currency('EUR', 100, items)
puts "100 PLN = #{to_foreign_currency}"

database = verify_database()

puts "Beginning transaction..."
database.transaction()

save_or_update_in_database(database, get_rate('EUR', items))

puts "Committed transaction..."
database.commit()


