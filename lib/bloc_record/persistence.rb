require 'sqlite3'
require 'bloc_record/schema'

module Persistence
  def self.included(base)
    base.extend(ClassMethods)
  end

  def save
    self.save! rescue false
  end

  def save!
    unless self.id
      self.id = self.class.create(BlocRecord::Utility.instance_variable_to_hash(self)).id
      BlocRecord::Utility.reload_obj(self)
      return true
    end

    fields = self.class.attributes.map { |col| "#{col}=#{BlocRecord::Utility.sql_strings(self.instance_variable_get("@#{col}"))}" }.join(",")
    self.class.connection.execute <<-SQL
      UPDATE #{self.class.table}
      SET #{fields}
      WHERE id = #{self.id};
    SQL

    true
  end

  def update_attribute(attribute, value)
    self.class.update(self.id, { attribute => value })
  end

  def update_attributes(updates)
    self.class.update(self.id, updates)
  end

  def destroy
    self.class.destroy(self.id)
  end

  def method_missing(m, *args)
    attribute = m.to_s
    attribute.slice! "update_"
    value = args[0]

    update_attribute(attribute, value)
  end

  module ClassMethods
    def update_all(updates)
      update(nil, updates)
    end

    def create(attrs)
      attrs = BlocRecord::Utility.convert_keys(attrs)
      attrs.delete("id")
      vals = attributes.map { |key| BlocRecord::Utility.sql_strings(attrs[key]) }

      connection.execute <<-SQL
        INSERT INTO #{table} (#{attributes.join ","})
        VALUES (#{vals.join ","});
      SQL

      data = Hash[attributes.zip attrs.values]
      data["id"] = connection.execute("SELECT last_insert_rowid();")[0][0]
      new(data)
    end

    def update(ids, updates)
      updates = BlocRecord::Utility.convert_keys(updates)
      updates.delete "id"
      updates_array = updates.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }

      if ids.class == Array && updates.class == Array
        updates_array = updates.map { |update| update.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}" }}
        sql_statements = []
        updates_array.each_with_index { |val, index| sql_statements << "UPDATE table SET #{updates_array[index] * ","} WHERE id = #{ids[index]};" }.join(" ")
        rows = connection.execute(sql_statements)
        return true
      elsif ids.class == Integer
        where_clause = "WHERE id = #{ids};"
      elsif ids.class == Array
        where_clause = ids.empty? ? ";" : "WHERE id IN (#{ids.join(",")});"
      else
        where_clause = ";"
      end

      connection.execute <<-SQL
        UPDATE #{table}
        SET #{updates_array * ","} #{where_clause}
      SQL
      true
    end

    def destroy(*id)
      if id.length > 1
        where_clause = "WHERE id IN (#{id.join(",")});"
      else
        where_clause = "WHERE id = #{id.first};"
      end
      connection.execute <<-SQL
        DELETE FROM #{table} #{where_clause}
      SQL

      true
    end

    def destroy_all(*conditions_hash)
      if conditions_hash.class == Hash
        conditions_hash = BlocRecord::Utility.convert_keys(conditions_hash)
        conditions = conditions_hash.map { |key, value| "#{key}=#{BlocRecord::Utility.sql_strings(value)}"}.join(" and ")
      elsif conditions_hash.class == String
        conditions = conditions_hash
      elsif conditions_hash.class == Array
        if conditions_hash.length == 0
          connection.execute <<-SQL
            DELETE FROM #{table};
          SQL
        else
          conditions_hash.each do |h|
            h.chomp!("?")
          end
          conditions = conditions_hash.join("")
        end
      end

      connection.execute <<-SQL
        DELETE FROM #{table}
        WHERE #{conditions};
      SQL

      true
    end
  end
end
