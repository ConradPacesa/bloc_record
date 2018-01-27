require 'sqlite3'
require 'pg'

module Connection
  def connection
    case BlocRecord.dbs
    when :sqlite
      @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
    when :pg
      @connection = PG.connect :dbname => "address_book", :user => "conrad", :password => "password"
    end
  end
end
