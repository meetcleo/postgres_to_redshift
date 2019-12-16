module PostgresToRedshift
  class Column
    DEFAULT_PRECISION = 19 # 99_999_999_999_999_999.99
    DEFAULT_SCALE = 2

    CAST_TYPES_FOR_COPY = {
      'text' => 'CHARACTER VARYING(65535)',
      'json' => 'CHARACTER VARYING(65535)',
      'jsonb' => 'CHARACTER VARYING(65535)',
      'bytea' => 'CHARACTER VARYING(65535)',
      'money' => "DECIMAL(#{DEFAULT_PRECISION},#{DEFAULT_SCALE})",
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
        case data_type
        when 'numeric'
          precision = (numeric_precision || DEFAULT_PRECISION).to_i + 1 # number of digits + the dot
          scale = numeric_scale || DEFAULT_SCALE

          %[CAST(RIGHT(ROUND("#{name}", #{scale})::text, #{precision}) AS #{data_type_for_copy}) AS #{name}]
        else
          %[CAST("#{name}" AS #{data_type_for_copy}) AS #{name}]
        end
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
      when 'numeric'
        precision = numeric_precision || DEFAULT_PRECISION
        scale = numeric_scale || DEFAULT_SCALE

        "#{type}(#{precision},#{scale})"
      else
        type
      end
    end

    def needs_type_cast?
      data_type != data_type_for_copy
    end
  end
end
