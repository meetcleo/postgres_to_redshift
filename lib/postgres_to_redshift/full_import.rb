module PostgresToRedshift
  class FullImport
    def initialize(table:, target_connection:, source_connection:, schema:)
      @table = table
      @target_connection = target_connection
      @schema = schema
      @tuning = Tuning.new(table: table, source_connection: source_connection)
    end

    def run
      puts "#{Time.now.utc} - Importing #{table.target_table_name}"

      # TRUNCATE cannot be rolled back
      target_connection.exec("DROP TABLE IF EXISTS #{table_name} CASCADE;")

      puts "#{Time.now.utc} - Creating #{table.target_table_name} with:\n#{create_table_statement}"
      target_connection.exec("#{create_table_statement};")

      # target_connection.exec("COPY #{table_name} FROM 's3://#{ENV['S3_DATABASE_EXPORT_BUCKET']}/export/#{table.target_table_name}.psv.gz' CREDENTIALS 'aws_access_key_id=#{ENV['S3_DATABASE_EXPORT_ID']};aws_secret_access_key=#{ENV['S3_DATABASE_EXPORT_KEY']}' GZIP TRUNCATECOLUMNS ESCAPE DELIMITER as '|';")

      target_connection.exec("ANALYZE #{table_name};")
    end

    private

    def table_name
      "#{schema}.#{target_connection.quote_ident(table.target_table_name)}"
    end

    def create_table_statement
      statement = "CREATE TABLE #{table_name} (#{table.columns_for_create})"
      return statement if table_name =~ /credit_score_reason_mappings/
      return statement if table_name == 'public."taggings"'

      statement += " DISTSTYLE KEY DISTKEY (#{distribution_key})" if distribution_key.present?
      statement += " SORTKEY(#{sort_keys.first})" if sort_keys.one?
      statement += " COMPOUND SORTKEY(#{sort_keys.join(',')})" if sort_keys.count > 1
      statement
    end

    def distribution_key
      tuning.distribution_key
    end

    def sort_keys
      tuning.sort_keys
    end

    attr_reader :table, :target_connection, :schema, :tuning
  end
end
