# PostgresToRedshift

This gem copies data from postgres to redshift. It's especially useful to copy data from postgres to redshift in heroku.

[![Build Status](https://travis-ci.org/kitchensurfing/postgres_to_redshift.svg?branch=master)](https://travis-ci.org/kitchensurfing/postgres_to_redshift)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'postgres_to_redshift'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres_to_redshift

## Usage

Set your source and target databases, as well as your s3 intermediary.

```bash
export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='testing-data'
export S3_DATABASE_EXPORT_ID='yourid'
export S3_DATABASE_EXPORT_KEY='yourkey'
export S3_DATABASE_EXPORT_BUCKET='some-bucket-to-use'

postgres_to_redshift
```

### Incremental Imports

It is possible to run an import that will pick up only records that have updated sine the last run of the import. It has the following caveats:

1. Does not apply deletions to the target table
1. Requires that the source table has either an `updated_at` or `created_at` field

Should you wish to enable incremental mode, set the following ENV:

```bash
export POSTGRES_TO_REDSHIFT_INCREMENTAL=true
```

It will record the start time of the last import in a local file and will import changes on or after that start time for subsequent imports.

### Dry Runs

It is possible to run the import in _dry run_ mode whereby the import will run, but will roll back any changes to the target tables.

```bash
export POSTGRES_TO_REDSHIFT_DRY_RUN=true
```

### Error handling

If an error is encountered during an import it will be handled as follows:

* Incremental imports: The import will be rolled back and retried from the beginning
* Full imports: The import will be rolled back to the previous table and the current table's import will be retried

An import will be attempted three times before giving up and raising the exception to the caller.

#### Transactions

For an _incremental_ import, the entire import process is performed in one database transaction to ensure that the data remains in a consistent state while the import is running as it is assumed that the incremental import will be running during business hours moving a relatively small amount of data. For a _full_ import, each table is imported in its own transaction as it is assumed that the full import is running outside of business hours and would be moving too large a volume of data to be performed in a single transaction.

### Performance tuning

Many of the options specified in the Redshift [docs](https://docs.aws.amazon.com/redshift/latest/dg/t_Creating_tables.html) are implemented.

- By default, PK and FK constraints are applied wherever possible in Redshift automatically.
- [Key distribution](https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html) and [compound sort keys](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html) will be defined if possible, based on the table's primary key. If table structure is meant to be optimised for the table specified in the `POSTGRES_TO_REDSHIFT_OPTIMISED_FOR_TABLE` env, then any tables with a foreign key to the optimised table will be distributed and sorted based on this foreign key column instead.

## Contributing

1. Fork it ( https://github.com/kitchensurfing/postgres_to_redshift/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
