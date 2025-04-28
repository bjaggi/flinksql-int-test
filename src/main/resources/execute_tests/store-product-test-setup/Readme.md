# Store Product Test Setup

This directory contains the test setup for store product functionality.

## Files

- `create_tables.sql`: Creates the necessary tables for the test setup
  - Creates the `Development.Digital-Public-Development.shared.digital.products.eligibility` table
  - Configures Kafka connector settings

- `drop_tables.sql`: Drops the tables created for testing
  - Drops the `Development.Digital-Public-Development.shared.digital.products.eligibility` table

- `insert_data.sql`: Contains the data insertion statements for testing
- `expected_op.csv`: Contains the expected output data for validation
- `external-digital-products-store-product-insert.sql`: Contains the external table creation and data insertion statements

## Test Flow

1. The test reads expected data from CSV files in each subdirectory
2. For each subdirectory:
   - Reads the expected output CSV file
   - Validates the file exists
   - Imports the data for comparison
3. Executes the Hybris store product query
4. Compares the actual results with expected results
5. Logs detailed comparison information for each row
6. Cleans up by dropping temporary tables

## Logging

The test provides detailed logging:
- Directory and file operations
- Data import status
- Query execution details
- Row-by-row comparison results
- Visual separators between test sections

## Usage

1. Run `create_tables.sql` to set up the required tables
2. Run `insert_data.sql` to populate the tables with test data
3. Execute the test queries
4. Compare the results with `expected_op.csv`
5. Run `drop_tables.sql` to clean up the test environment
