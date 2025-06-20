# Flink SQL Integration Testing Framework

## Table of Contents
- [Flink SQL Integration Testing Framework](#flink-sql-integration-testing-framework)
  - [Table of Contents](#table-of-contents)
  - [Use Case](#use-case)
  - [Overview](#overview)
  - [Test Resources Structure](#test-resources-structure)
  - [Test Scenarios](#test-scenarios)
  - [Test Folder Contents](#test-folder-contents)
  - [Components](#components)
  - [Test Flow](#test-flow)
  - [Logging](#logging)
  - [Configuration](#configuration)
    - [Cloud Properties](#cloud-properties)
    - [Test Resources Location](#test-resources-location)
  - [Running Tests](#running-tests)
    - [Manual Execution](#manual-execution)
    - [Via IDE](#via-ide)
    - [Via Docker](#via-docker)
      - [Prerequisites](#prerequisites)
      - [Configuration](#configuration-1)
      - [Custom Test Resources](#custom-test-resources)
    - [Test Output](#test-output)
  - [CI/CD Integration](#cicd-integration)
  - [Known Limitations](#known-limitations)
  - [Best Practices](#best-practices)

## Use Case
This framework enables automated integration testing of Flink SQL applications in Confluent Cloud. It helps ensure that your SQL queries work correctly with real data before deployment to production.

Key benefits:
- Automated validation of Flink SQL queries
- Real-time testing with Confluent Cloud
- Support for both manual and CI/CD testing workflows
- Flexible test data management

## Overview
A Java-based testing framework that uses Flink's Table API to validate SQL queries against a Confluent Cloud cluster. The framework supports:
- Programmatic and file-based test data insertion
- Automated test execution via GitHub Actions
- Customizable test resource locations
- Comprehensive assertion capabilities

## Test Resources Structure
The test resources are organized in the `src/main/resources/execute_tests` directory (configurable). This directory can contain multiple test scenarios, each representing a different test case or feature to validate.

```
src/
├── main/
│   ├── java/
│   │   └── io/confluent/flink/examples/
│   │       ├── helper/
│   │       │   ├── SqlReader.java         # SQL file operations and execution
│   │       │   ├── RowComparator.java     # Row comparison utilities
│   │       │   ├── DataImporter.java      # CSV data import utilities
│   │       │   └── TestConstants.java     # Test configuration constants
│   │       └── HybrisStoreProductService.java
│   └── resources/
│       ├── execute_tests/                 # Root directory for all test scenarios
│       │   ├── products.price-current-release/  # Test scenario 1
│       │   │   ├── drop_tables/
│       │   │   ├── create_tables/
│       │   │   ├── insert_data.sql
│       │   │   ├── execute_query.sql
│       │   │   └── expected_op.csv
│       │   └── another.test.scenario/     # Test scenario 2
│       │       ├── drop_tables/
│       │       ├── create_tables/
│       │       ├── insert_data.sql
│       │       ├── execute_query.sql
│       │       └── expected_op.csv
│       └── cloud.properties               # Confluent Cloud configuration
└── test/
    └── java/
        └── io/confluent/flink/examples/
            └── HybrisStoreProductServiceTest.java
```

## Test Scenarios
The framework supports multiple test scenarios, each in its own directory under `execute_tests/`. Each scenario represents a complete test case with its own set of tables, data, and expected results. This allows you to:
- Test different features independently
- Maintain separate test data for each scenario
- Run specific scenarios as needed
- Keep test resources organized by feature or use case

## Test Folder Contents
Each test scenario directory must follow this structure:
- `drop_tables(📁)`: Contains SQL files to drop tables. Multiple files are supported and will be executed in sequence([refer sample structure](https://github.com/bjaggi/flinksql-int-test/tree/code_with_generic_framework/src/main/resources/execute_tests/products.price-current-release)). 
- `create_tables📁`: Contains SQL files to create tables. Multiple files are supported and will be executed in sequence([refer sample structure](https://github.com/bjaggi/flinksql-int-test/tree/code_with_generic_framework/src/main/resources/execute_tests/products.price-current-release)). 
- `insert_data.sql`: Contains INSERT statements to populate test data
- `execute_query.sql`: The main query to be tested
- `expected_op.csv`: Expected output data for validation

The framework will automatically discover and execute all scenarios under the `execute_tests` directory, making it easy to add new test cases without modifying the test code.

## Components
1. **Java Test Framework**
   - Unit tests in Java using JUnit
   - Flink Table API integration
   - Confluent Cloud connectivity

2. **Test Resources**
   - SQL files for data insertion
   - Test case configurations
   - Cloud connection properties

## Test Flow
1. The test reads expected data from CSV files in each subdirectory
2. For each subdirectory:
   - Reads the expected output CSV file
   - Validates the file exists
   - Imports the data for comparison
3. Executes the Hybris store product query
4. Compares the actual results with expected results using string-based set comparison:
   - Converts each row to string representation
   - Filters out null or empty rows
   - Compares sets for equality regardless of order
   - Shows detailed differences (missing/extra values)
5. Logs detailed comparison information for each row
6. Cleans up by dropping temporary tables

## Logging
The test provides detailed logging:
- Directory and file operations
- Data import status
- Query execution details
- Row-by-row comparison results
- Visual separators between test sections

## Configuration

### Cloud Properties
Create `resources/cloud.properties`:
```properties
client.organization-id=your-org-id
client.environment-id=your-env-id
client.flink-api-key=your-api-key
client.flink-api-secret=your-api-secret
client.compute-pool-id=your-pool-id
client.cloud=your-cloud
client.region=your-region
```

### Test Resources Location
Customize the test resources location using:

1. System Property:
```bash
mvn test -Dflink.test.resources.path=/custom/path/to/resources
```

2. Environment Variable:
```bash
export FLINK_TEST_RESOURCES_PATH=/custom/path/to/resources
mvn test
```

## Running Tests

### Manual Execution
```bash
mvn package
# or
mvn test
```

### Via IDE
Run `HybrisStoreProductServiceTest.java` directly in your IDE.

### Via Docker
The project includes Docker support for containerized test execution.

#### Prerequisites
- Docker
- Docker Compose

#### Configuration
1. Create a `.env` file in the project root:
```env
CLIENT_ORGANIZATION_ID=your-org-id
CLIENT_ENVIRONMENT_ID=your-env-id
CLIENT_FLINK_API_KEY=your-api-key
CLIENT_FLINK_API_SECRET=your-api-secret
CLIENT_COMPUTE_POOL_ID=your-pool-id
CLIENT_CLOUD=your-cloud
CLIENT_REGION=your-region
```

2. Build and run using Docker Compose:
```bash
# Build the Docker image
docker-compose build

# Run the tests
docker-compose up

# Run and remove containers after completion
docker-compose up --abort-on-container-exit
```

3. Access test results:
- Test results are available in the `test-results` directory
- Logs are streamed to the console

#### Custom Test Resources
Mount your custom test resources:
```bash
docker-compose run -v /path/to/your/tests:/app/resources/execute_tests flink-test
```

### Test Output
- Job ID will be logged (e.g., `job id: [table-api-2025-03-25-104344-dd870732-f129-436c-b433-999f3319aaed-sql]`)
- Check test results in the console output
- Failed tests will show data mismatches or timeouts

## CI/CD Integration
- Automatically run tests on SQL code changes
- GitHub Actions integration available
- Optional: Gate deployments based on test results

## Known Limitations
- CTAS (Create Table As Select) is not available in Table API version 1.20-50
- Expected in Q2/2025
- Current workaround: Using `SELECT` instead of `CREATE TABLE AS SELECT`
- There may be timeout errors to schema registry, if you see these errors either retry or delete the kafka topic
- The string-based set comparison has the following limitations:
  - Special characters in strings may affect comparison
  - Complex data types (arrays, maps, nested structures) may not compare correctly
  - Timestamp formats must match exactly
  - Null values are filtered out
  - Order of fields in rows is not considered
  If you encounter comparison issues, check the actual vs expected data format and adjust accordingly.

## Best Practices
1. Create one test case per scenario
2. Use appropriate test data for each case
3. Keep test resources organized by feature
4. Use meaningful test and file names
5. Document expected results
