#!/usr/bin/env ruby
## Copyright Grzegorz Blaszczyk Consulting 2011

MaxRSSItems = 100

DbName = 'currency_rates'
DbTable = 'rates'
DbHost = '127.0.0.1'
DbPort = 5432
DbUser = 'currency_rates'
DbPass = 'currency_rates'

### DO NOT EDIT BELOW THIS LINE ###

require 'pg'
require 'rss'

def fetch_rates()
  return RSS::Parser.parse(open('http://rss.nbp.pl/kursy/TabelaA.xml').read, false).items[0..MaxRSSItems-1]
end

def get_rate(currency_code, items)

  items.each do |item|
    plain_description = item.description.gsub(/<\/?[^>]*>/, "")
    puts plain_description
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
  database = PGconn.connect(DbHost, DbPort, '', '', DbName, DbUser, DbPass)

  result = database.exec( "SELECT count(tablename) FROM pg_catalog.pg_tables WHERE tablename = '#{DbTable}'")
  if (result.getvalue(0,0) == '0')
    puts "Creating table #{DbTable}..."
    database.exec("CREATE TABLE #{DbTable} (
      id SERIAL PRIMARY KEY,
      code TEXT,
      table_no TEXT, 
      date_created DATE,
      value REAL
    )")
  else
    puts "Table #{DbTable} exists..."
  end

  return database
end

def save_or_update_in_database(database, rate)

  puts "Filling database..."
  select_query = "select value FROM #{DbTable} WHERE code = $1 AND date_created = $2 ORDER BY date_created DESC LIMIT 1"
  insert_query = "insert into #{DbTable} (code,table_no,date_created,value) values ($1,$2,$3,$4)"
  
  select_statement = database.prepare('select_query', select_query)
  insert_statement = database.prepare('insert_query', insert_query)

  rows = database.exec_prepared('select_query', [rate['code'], rate['date']])
  if !rows.nil? and rows.ntuples > 0
    puts "Data up to date..."
  else
    puts "Executing first insert for #{rate['code']} today..."
    database.exec_prepared('insert_query', [rate['code'], rate['table_no'], rate['date'], rate['value']])
  end  
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

puts "Start manipulating with database..."

save_or_update_in_database(database, get_rate('EUR', items))

puts "Finished manipulating with database..."


