# Store Eligibility Location Test

This directory contains the test setup for store eligibility location functionality.

## Files

- `create_tables.sql`: Creates the necessary tables for the test setup
  - Creates the `Development.Digital-Public-Development.shared.digital.stores.location` table
  - Configures Kafka connector settings

- `drop_tables.sql`: Drops the tables created for testing
  - Drops the `Development.Digital-Public-Development.shared.digital.stores.location` table

- `insert_data.sql`: Contains the data insertion statements for testing
- `expected_op.csv`: Contains the expected output data for validation

## Usage

1. Run `create_tables.sql` to set up the required tables
2. Run `insert_data.sql` to populate the tables with test data
3. Execute the test queries
4. Compare the results with `expected_op.csv`
5. Run `drop_tables.sql` to clean up the test environment
