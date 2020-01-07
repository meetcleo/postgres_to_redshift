module PostgresToRedshift
  class Key
    PRIMARY_KEY = 'p'.freeze
    FOREIGN_KEY = 'f'.freeze
    PRIMARY_KEY_DECOMPOSER = /PRIMARY KEY \((?<source_column_name>\w+)\)/i.freeze
    FOREIGN_KEY_DECOMPOSER = /FOREIGN KEY \((?<source_column_name>\w+)\) REFERENCES (?<target_table_name>\w+)\((?<target_column_name>\w+)\)/i.freeze

    def initialize(attributes:)
      @attributes = attributes
    end

    def to_sql
      case key_type
      when PRIMARY_KEY
        primary_key_sql
      when FOREIGN_KEY
        foreign_key_sql
      else
        raise "Unsupported key type #{key_type}"
      end
    end

    def primary?
      key_type == PRIMARY_KEY
    end

    def source_column_name
      decomposed_key[:source_column_name]
    end

    def target_table_name
      return if primary?

      decomposed_key[:target_table_name]
    end

    def target_column_name
      return if primary?

      decomposed_key[:target_column_name]
    end

    def touches_table?(name)
      [target_table_name, table_name].compact.map(&:downcase).include?(name.downcase)
    end

    def key_name
      attributes['name']
    end

    private

    attr_reader :attributes

    def decomposed_key
      regex = case key_type
              when PRIMARY_KEY
                PRIMARY_KEY_DECOMPOSER
              when FOREIGN_KEY
                FOREIGN_KEY_DECOMPOSER
              else
                raise "Unsupported key type #{key_type}"
              end

      regex.match(key_definition)
    end

    def key_definition
      attributes['key_definition']
    end

    def foreign_key_sql
      "ALTER TABLE #{table_name} ADD CONSTRAINT #{key_name} #{key_definition}"
    end

    def primary_key_sql
      "ALTER TABLE #{table_name} ADD #{key_definition}"
    end

    def key_type
      attributes['key_type']
    end

    def table_name
      attributes['table_name']
    end
  end
end
