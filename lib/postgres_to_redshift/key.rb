module PostgresToRedshift
  class Key
    PRIMARY_KEY = 'p'.freeze
    FOREIGN_KEY = 'f'.freeze

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

    private

    attr_reader :attributes

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

    def key_name
      attributes['name']
    end
  end
end
