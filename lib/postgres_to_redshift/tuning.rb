module PostgresToRedshift
  class Tuning
    DEFAULT_SORT_KEYS = %w[created_at] # We often filter and sort by these columns

    def initialize(table:, source_connection:)
      @table = table
      @table_name = table.target_table_name.downcase
      @optimised_for_table_name = ENV['POSTGRES_TO_REDSHIFT_OPTIMISED_FOR_TABLE']&.downcase&.strip || table_name
      @keys = Keys.new(source_connection: source_connection, tables: [table_name, optimised_for_table_name].uniq).all
    end

    # If there is a reference to the optimised table, sort on this column in order to optimise any join with optimised table
    # Else, sort by PK to optimise any joins or filter on this table
    def sort_keys
      sort_key_column_names = if can_optimise_for_different_table?
                           [foreign_key_column_name_to_optimised_table]
                         else
                           [primary_key_column_name].compact
                         end

      sort_key_column_names + DEFAULT_SORT_KEYS.select { |column_name| table_includes_column?(column_name) }
    end

    # If there is a reference to the optimised table, distribute by this column in order to optimise any join with optimised table
    # Else, distribute by PK to optimise any joins or filter on this table
    def distribution_key
      return foreign_key_column_name_to_optimised_table if can_optimise_for_different_table?

      primary_key_column_name
    end

    private

    def table_includes_column?(column_name)
      table.column_names.map(&:downcase).include?(column_name)
    end

    def can_optimise_for_different_table?
      optimised_for_different_table? && foreign_keys_to_optimised_table.any?
    end

    def optimised_for_different_table?
      optimised_for_table_name != table_name
    end

    def primary_key_column_name
      keys.detect(&:primary?)&.source_column_name
    end

    def foreign_key_column_name_to_optimised_table
      foreign_keys_to_optimised_table.first&.source_column_name
    end

    def foreign_keys_to_optimised_table
      keys.reject(&:primary?).select { |key| key.target_table_name.downcase == optimised_for_table_name }
    end

    attr_reader :keys, :optimised_for_table_name, :table_name, :table
  end
end
