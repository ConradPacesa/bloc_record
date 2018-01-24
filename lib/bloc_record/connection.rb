require 'sqlite3'
require 'pg'

module Connection 
  def connection
    @connection ||= SQLite3::Database.new(BlocRecord.database_filename)
  end
end 