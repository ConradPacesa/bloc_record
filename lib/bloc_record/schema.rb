require 'sqlite3'
require 'pg'
require 'bloc_record/utility'

module Schema
  def table
    BlocRecord::Utility.underscore(name)
  end

  def schema
    case BlocRecord.dbs
    when :sqlite
      unless @schema
        @schema = {}
        connection.table_info(table) do |col|
          @schema[col["name"]] = col["type"]
        end
      end
    when :pg
      unless @schema
        @schema = {}
        rows = connection.exec <<-SQL
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_name = '#{table}';
        SQL
        rows.each do |row|
          @schema[row["column_name"]] = row["data_type"]
        end
      end
    end
    @schema
  end

  def columns
    schema.keys
  end

  def attributes
    columns - ["id"]
  end

  def count
    connection.execute(<<-SQL)[0][0]
      SELECT COUNT(*) FROM #{table}
    SQL
  end
end
