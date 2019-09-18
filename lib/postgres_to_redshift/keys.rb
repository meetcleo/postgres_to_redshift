module PostgresToRedshift
  class Keys
    def initialize(source_connection:, tables: nil)
      @source_connection = source_connection
      @tables = tables
    end

    def all
      all_keys
    end

    private

    attr_reader :source_connection, :tables

    def all_keys
      source_connection.exec(find_all_keys_sql).map do |key_attributes|
        Key.new(attributes: key_attributes)
      end.compact
    end

    def find_all_keys_sql
      sql = <<~SQL
        select
          conrelid::regclass AS table_name,
          conname as name,
          pg_get_constraintdef(oid) as key_definition,
          contype as key_type
        from pg_constraint
        where connamespace = 'public'::regnamespace
      SQL

      if tables
        table_names = "'" + tables.join("', '") + "'"
        sql += "and conrelid::regclass::text in (#{table_names})\n"
        table_names = table_names.gsub(',', '|').delete("'")
        sql += "and (contype = '#{Key::PRIMARY_KEY}' or (contype = '#{Key::FOREIGN_KEY}' and pg_get_constraintdef(oid) similar to '%REFERENCES (#{table_names})%'))\n"
      else
        sql += "where contype in ('#{Key::FOREIGN_KEY}', '#{Key::PRIMARY_KEY}')\n"
      end

      sql += 'order by contype desc;'
      sql
    end
  end
end
