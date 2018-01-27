module BlocRecord
  def self.connect_to(dbs)
    @dbs = dbs
    if dbs == :sqlite
      @database_filename = "db/address_bloc.sqlite"
    end
  end

  def self.database_filename
    @database_filename
  end

  def self.dbs
    @dbs
  end
end
