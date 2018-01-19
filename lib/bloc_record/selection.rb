require 'sqlite3'

module Selection
  def find(*ids)
    ids.each do |id|
      validate_int(id)
    end

    if ids.length == 1 
      find_one(ids.first)
    else 
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id IN (#{ids.join(",")});
      SQL

      rows_to_array(rows)
    end 
  end

  def find_one(id)
    validate_int(id)

    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE id = #{id};
    SQL

    init_object_from_row(row)
  end

  def find_by(attribute, value)
    if !attribute.is_a?(String) || !value.is_a?(String)
      raise ArgumentError, "Attribute and value must be strings."
    end

    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    init_object_from_row(row)
  end

  def take(num=1)
    validate_int(num)

    if num > 1
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        ORDER BY random() 
        LIMIT #{num};
      SQL

      rows_to_array(rows)
    else 
      take_one
    end 
  end

  def take_one
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY random()
      LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def first
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id ASC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def last
    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      ORDER BY id DESC LIMIT 1;
    SQL

    init_object_from_row(row)
  end

  def all
    rows = connection.execute <<-SQL
      SELECT #{columns.join ","} FROM #{table};
    SQL

    rows_to_array(rows)
  end

  def method_missing(m, *args, &block)
    attribute = m.to_s
    attribute.slice! "find_by_"
    value = args[0]

    row = connection.get_first_row <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{attribute} = #{BlocRecord::Utility.sql_strings(value)};
    SQL

    init_object_from_row(row)
  end

  def find_each(start: 0, batch_size: 1000)
    offset = start

    loop do 
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id > #{offset}
        ORDER BY id ASC LIMIT #{batch_size};
      SQL

      result = rows_to_array(rows)
      result.each do |r|
        yield(r)
        if r.id == self.last.id
          return
        end
      end
      offset += batch_size
    end
  end

  def find_in_batches(start: 0, batch_size: 1000, &block)
    offset = start

    loop do
      rows = connection.execute <<-SQL
        SELECT #{columns.join ","} FROM #{table}
        WHERE id > #{offset}
        ORDER BY id ASC LIMIT #{batch_size};
      SQL

      result = rows_to_array(rows)
      block.call result

      if result.last.id == self.last.id
        return
      end
      offset += batch_size
    end
  end

  def where(*args)
    if args.count > 1 
      expression = args.shift
      params = args
    else
      case args.first
      when String
        expression = args.first
      when Hash
        expression_hash = BlocRecord::Utility.convert_keys(args.first)
        expression = expression_hash.map {|key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      end
    end

    sql = <<-SQL
      SELECT #{columns.join ","} FROM #{table}
      WHERE #{expression};
    SQL

    rows = connection.execute(sql, params)
    rows_to_array(rows)
  end

  def order(*args)
    case args.first
    when String
      order = args.join(", ")
    when Hash
      order = args.first.map { |key, value| "#{key} #{value}" }.join(", ")
    end

    rows = connection.execute <<-SQL
      SELECT * FROM #{table}
      ORDER BY #{order};
    SQL
    rows_to_array(rows)
  end

  def join(*args)
    if args.count > 1
      joins = args.map { |arg| "INNER JOIN #{arg} ON #{arg}.#{table}_id = #{table}.id" }.join(" ")
      rows = connection.execute <<-SQL
        SELECT * FROM #{table} #{joins};
      SQL
    else 
      case args.first
      when String
        rows = connection.execute <<-SQL
          SELECT * FROM #{table} #{BlocRecord::Utility.sql_strings(args.first)}; 
        SQL
      when Symbol
        rows = connection.execute <<-SQL
          SELECT * FROM #{table}
          INNER JOIN #{args.first} ON #{args.first}.#{table}_id = #{table}.id;
        SQL
      when Hash
        joins = (args.first.map { |key, value| "INNER JOIN #{key} ON #{key}.#{table}_id = #{table}.id" }.join(" ")) + " " + ((args.first.map { |key, value| "INNER JOIN #{value} ON #{value}.#{key}_id = #{key}.id" }.join(" ")))
      end
    end

    rows_to_array(rows)
  end

  private
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end

  def rows_to_array(rows)
    collection = BlocRecord::Collection.new
    rows.each { |row| collection << new(Hash[columns.zip(row)]) }
    collection
  end

  def validate_int(int)
    if !int.is_a?(Integer)
      raise ArgumentError, "#{int} must be of type integer."
    end
    if int < 1
      raise ArgumentError, "#{int} must be greater than 0."
    end
  end
end