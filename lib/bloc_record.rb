module BlocRecord
  def self.connect_to(dbs, database_filename=nil)
    @dbs = dbs
    @database_filename = database_filename
  end

  def self.database_filename
    @database_filename
  end

  def self.dbs
    @dbs
  end
end
