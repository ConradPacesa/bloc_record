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

  private
  def init_object_from_row(row)
    if row
      data = Hash[columns.zip(row)]
      new(data)
    end
  end

  def rows_to_array(rows)
    rows.map { |row| new(Hash[columns.zip(row)])}
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