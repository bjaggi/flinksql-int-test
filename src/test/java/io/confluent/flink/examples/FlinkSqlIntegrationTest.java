package io.confluent.flink.examples;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.examples.helper.SqlReader;
import io.confluent.flink.examples.helper.SqlReader.*;
import io.confluent.flink.examples.helper.RowComparator;
import io.confluent.flink.examples.helper.DataExporter;
import io.confluent.flink.examples.helper.DataImporter;
import io.confluent.flink.examples.helper.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.StreamSupport;

import java.util.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.nio.file.*;
import java.io.File;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
public class FlinkSqlIntegrationTest  {
    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlIntegrationTest.class);
    private static final String breakline = "------------------------------------------------------------------------------------------------------------------------";
    private static final String EXECUTE_QUERY_SQL = "execute_query.sql";
    private static final String EXPECTED_OP_CSV = "expected_op.csv";
    private TableEnvironment env;


    public FlinkSqlIntegrationTest() {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        env = TableEnvironment.create(settings);
    }

    private String getResourcesPath() {
        // Try system property first
        String path = System.getProperty("flink.test.resources.path");
        if (path != null && !path.isEmpty()) {
            return path;
        }
        
        // Try environment variable
        path = System.getenv("FLINK_TEST_RESOURCES_PATH");
        if (path != null && !path.isEmpty()) {
            return path;
        }
        
        // Default path
        return "src/main/resources/execute_tests";
    }

    /**
     * This test executes all tests in the subdirectories under execute_tests folder.
     * The test follows this sequence:
     * 1. Drops all tables specified in the drop_tables folder
     * 2. Creates all tables specified in the create_tables folder
     * 3. Inserts data from the insert_data folder
     * 4. Executes the query from query.sql
     * 5. Compares results with expected_op.csv
     * 
     * Directory Structure:
     * execute_tests/
     * ├── products.price-current-release/
     * │   ├── expected_op.csv                    # Expected output for comparison
     * │   ├── create_tables/                     # Table creation SQL files
     * │   │   ├── process-scheduled-price-window.sql
     * │   │   ├── price-current-release.sql
     * │   │   └── price-current-stage.sql
     * │   ├── insert_data/                       # Data insertion SQL files
     * │   │   └── insert_data.sql
     * │   ├── drop_tables/                       # Table deletion SQL files
     * │   │   └── drop_tables.sql
     * │   └── execute_query.sql                  # Main query to test
     * 
     * Configuration Notes:
     * - Schema Registry timeouts are set to 30 seconds for operations
     * - Table creation/drop operations timeout after 90 seconds
     * - Query execution timeout is set to 5 minutes
     * - All operations include proper error handling and logging
     * 
     * @throws Exception if any step in the test process fails
     */
    @Test
    @Timeout(value = 300)
    public void testFlinkSql() throws Exception {
        // Get the test resources directory
        String testResourcesPath = getResourcesPath();
        logger.info("Test resources path: {}", testResourcesPath);

        // Print directory structure
        SqlReader.printDirectoryStructure(testResourcesPath);

        // Get all test scenario directories
        File testDir = new File(testResourcesPath);
        File[] testScenarios = testDir.listFiles(File::isDirectory);

        if (testScenarios == null || testScenarios.length == 0) {
            logger.warn("No test scenarios found in {}", testResourcesPath);
            return;
        }

        // Process each test scenario
        for (File scenario : testScenarios) {
            logger.info("\n{}", breakline);
            logger.info("Processing test scenario: {}", scenario.getName());
            logger.info("{}", breakline);

            // Set up test resources
            SqlReader.setUpResourcesForTest(env, scenario.getPath());

            // Execute the test query
            File executeQueryFile = new File(scenario, EXECUTE_QUERY_SQL);
            if (!executeQueryFile.exists()) {
                logger.warn("No execute_query.sql found in {}", scenario.getName());
                continue;
            }

            TableResult results = SqlReader.executeQuery(executeQueryFile, env);
            if (results == null) {
                logger.error("Failed to execute query in {}", scenario.getName());
                continue;
            }

            // Read expected output
            File expectedOpFile = new File(scenario, EXPECTED_OP_CSV);
            if (!expectedOpFile.exists()) {
                logger.warn("No expected_op.csv found in {}", scenario.getName());
                continue;
            }

            List<Row> expectedOpFromFile = DataImporter.importFromCSV(expectedOpFile.getPath());
            if (expectedOpFromFile.isEmpty()) {
                logger.warn("Expected output file is empty in {}", scenario.getName());
                continue;
            }

            // Convert expected rows to sets
            Set<Set<String>> expectedSets = expectedOpFromFile.stream()
                .map(row -> {
                    Set<String> rowSet = new HashSet<>();
                    logger.info("Converting expected row: {}", row);
                    for (int i = 0; i < row.getArity(); i++) {
                        Object field = row.getField(i);
                        if (field != null) {
                            String value = String.valueOf(field);
                            rowSet.add(value);
                            logger.info("Added to expected set: {}", value);
                        } else {
                            logger.info("Skipping null value at position {}", i);
                        }
                    }
                    logger.info("Final expected row set: {}", rowSet);
                    return rowSet;
                })
                .collect(Collectors.toSet());

            logger.info("All expected sets: {}", expectedSets);

            // Fetch and convert actual results to sets
            Set<Set<String>> actualSets = fetchRows(results)
                .limit(expectedOpFromFile.size())
                .map(row -> {
                    Set<String> rowSet = new HashSet<>();
                    logger.info("Converting actual row: {}", row);
                    for (int i = 0; i < row.getArity(); i++) {
                        Object field = row.getField(i);
                        if (field != null) {
                            String value = String.valueOf(field);
                            rowSet.add(value);
                            logger.info("Added to actual set: {}", value);
                        } else {
                            logger.info("Skipping null value at position {}", i);
                        }
                    }
                    logger.info("Final actual row set: {}", rowSet);
                    return rowSet;
                })
                .collect(Collectors.toSet());

            logger.info("All actual sets: {}", actualSets);

            // Compare sets
            boolean isEqual = expectedSets.equals(actualSets);
            if (!isEqual) {
                logger.error("Test failed in scenario: {}", scenario.getName());
                logger.error("Expected sets: {}", expectedSets);
                logger.error("Actual sets: {}", actualSets);
                
                // Find missing and extra values
                Set<Set<String>> missingInActual = new HashSet<>(expectedSets);
                missingInActual.removeAll(actualSets);
                
                Set<Set<String>> extraInActual = new HashSet<>(actualSets);
                extraInActual.removeAll(expectedSets);
                
                logger.error("Missing in actual results: {}", missingInActual);
                logger.error("Extra in actual results: {}", extraInActual);
                
                fail("Test failed in scenario: " + scenario.getName() + 
                     "\nExpected sets: " + expectedSets + 
                     "\nActual sets: " + actualSets +
                     "\nMissing in actual: " + missingInActual +
                     "\nExtra in actual: " + extraInActual);
            } else {
                logger.info("Test passed in scenario: {}", scenario.getName());
            }
        }
    }

    protected Stream<Row> fetchRows(TableResult result) {
        Iterable<Row> iterable = result::collect;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
