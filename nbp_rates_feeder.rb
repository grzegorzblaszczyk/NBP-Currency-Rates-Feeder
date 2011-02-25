#!/usr/bin/env ruby
## Copyright Grzegorz Blaszczyk Consulting 2011

MaxRSSItems = 100

DbName = 'currency_rates'
DbTable = 'rates'
DbHost = '127.0.0.1'
DbPort = 5432
DbUser = 'currency_rates'
DbPass = 'currency_rates'
DbType = 'pg'

### DO NOT EDIT BELOW THIS LINE ###

require 'pg'
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

def verify_database_pgsql
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

  select_query = "select value FROM #{DbTable} WHERE code = $1 AND date_created = $2 ORDER BY date_created DESC LIMIT 1"
  insert_query = "insert into #{DbTable} (code,table_no,date_created,value) values ($1,$2,$3,$4)"
  
  select_statement = database.prepare('select_query', select_query)
  insert_statement = database.prepare('insert_query', insert_query)

  return database
end

def verify_database_sqlite
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

def save_or_update_in_database_pgsql(database, rate)
  puts "Filling database with #{rate['code']}..."
  rows = database.exec_prepared('select_query', [rate['code'], rate['date']])
  if !rows.nil? and rows.ntuples > 0
    puts "Data up to date..."
  else
    puts "Executing first insert for #{rate['code']} today..."
    database.exec_prepared('insert_query', [rate['code'], rate['table_no'], rate['date'], rate['value']])
  end  
end

def save_or_update_in_database_sqlite(database, rate)
  puts "Filling database with #{rate['code']}..."
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

def main(type)
  items = fetch_rates()
  if (type == 'pg')
    database = verify_database_pgsql()
    puts "Start manipulating with database..."
    save_or_update_in_database_pgsql(database, get_rate('EUR', items))
    save_or_update_in_database_pgsql(database, get_rate('USD', items))
    save_or_update_in_database_pgsql(database, get_rate('CHF', items))
    save_or_update_in_database_pgsql(database, get_rate('GBP', items))
    puts "Finished manipulating with database..."
  elsif (type == 'sqlite')
    database = verify_database_sqlite()
    puts "Beginning transaction..."
    database.transaction()
    save_or_update_in_database_sqlite(database, get_rate('EUR', items))
    save_or_update_in_database_sqlite(database, get_rate('USD', items))
    save_or_update_in_database_sqlite(database, get_rate('CHF', items))
    save_or_update_in_database_sqlite(database, get_rate('GBP', items))
    puts "Committed transaction..."
    database.commit()
  end
end

main(DbType)

## Testing ###
#rate = get_rate('EUR', items)
#puts "Table: #{rate['table_no']}"
#puts "Date: #{rate['date']}"
#puts "1 EUR = #{rate['value']}"

#from_foreign_currency = from_foreign_currency('EUR', 100, items)
#puts "100 EUR = #{from_foreign_currency}"

#to_foreign_currency = to_foreign_currency('EUR', 100, items)
#puts "100 PLN = #{to_foreign_currency}"
## End testing ###


