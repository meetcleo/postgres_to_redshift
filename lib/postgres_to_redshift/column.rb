module PostgresToRedshift
  class Column
    CAST_TYPES_FOR_COPY = {
      'text' => 'CHARACTER VARYING(65535)',
      'json' => 'CHARACTER VARYING(65535)',
      'jsonb' => 'CHARACTER VARYING(65535)',
      'bytea' => 'CHARACTER VARYING(65535)',
      'money' => 'DECIMAL(19,2)',
      'numeric' => 'DECIMAL',
      'oid' => 'CHARACTER VARYING(65535)',
      'ARRAY' => 'CHARACTER VARYING(65535)',
      'USER-DEFINED' => 'CHARACTER VARYING(65535)',
      'uuid' => 'CHARACTER VARYING(36)'
    }.freeze

    def initialize(attributes:)
      @attributes = attributes
    end

    def name
      attributes['column_name']
    end

    def name_for_copy
      if needs_type_cast?
        %[CAST("#{name}" AS #{data_type_for_copy}) AS #{name}]
      else
        %("#{name}")
      end
    end

    def data_type
      attributes['data_type']
    end

    def numeric_scale
      attributes['numeric_scale']
    end

    def numeric_precision
      attributes['numeric_precision']
    end

    def not_null?
      attributes['is_nullable'].to_s.downcase == 'no'
    end

    def data_type_for_copy
      type = CAST_TYPES_FOR_COPY[data_type] || data_type
      handle_additional_type_attributes(type)
    end

    private

    attr_reader :attributes

    def handle_additional_type_attributes(type)
      case type
      when 'DECIMAL'
        return type unless numeric_precision && numeric_scale

        "#{type}(#{numeric_precision},#{numeric_scale})"
      else
        type
      end
    end

    def needs_type_cast?
      data_type != data_type_for_copy
    end
  end
end
