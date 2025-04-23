# Flink SQL Integration Testing Framework

## Table of Contents
- [Flink SQL Integration Testing Framework](#flink-sql-integration-testing-framework)
  - [Table of Contents](#table-of-contents)
  - [Use Case](#use-case)
  - [Overview](#overview)
  - [Components](#components)
  - [Test Resources Structure](#test-resources-structure)
    - [Test Folder Contents](#test-folder-contents)
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

## Components
1. **Java Test Framework**
   - Unit tests in Java using JUnit
   - Flink Table API integration
   - Confluent Cloud connectivity

2. **Test Resources**
   - SQL files for data insertion
   - Test case configurations
   - Cloud connection properties

## Test Resources Structure
The test resources are organized in the `src/main/resources/execute_tests` directory (configurable):

```
execute_tests/
├── digital-stores-insert/        # Digital store tests
│   ├── insert_data.sql          # Store test data
│   └── shared-digital-stores-stores-insert.sql
├── store-eligibility-location/   # Eligibility tests
│   └── insert_data.sql
└── store-product-test-setup/    # Product tests
    └── insert_data.sql
```

### Test Folder Contents
- `insert_data.sql`: Test data INSERT statements
- Test-specific SQL files
- Configuration files as needed

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

## Best Practices
1. Create one test case per scenario
2. Use appropriate test data for each case
3. Keep test resources organized by feature
4. Use meaningful test and file names
5. Document expected results
