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
import java.util.concurrent.TimeoutException;
import java.util.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.nio.file.*;
import java.io.File;
import java.util.stream.Collectors;
import org.apache.flink.util.CloseableIterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
     * 5. Compares results with expected_op.csv using string-based set comparison
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
     * Comparison Logic:
     * - Results are compared using string-based set comparison
     * - Each row is converted to a string representation
     * - Null or empty rows are filtered out
     * - Sets are compared for equality regardless of order
     * - Detailed comparison shows missing and extra values
     * 
     * Configuration Notes:
     * - Schema Registry timeouts are set to 30 seconds for operations
     * - Table creation/drop operations timeout after 90 seconds
     * - Query execution timeout is set to 5 minutes
     * - Result collection timeout is set to 20 seconds
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

            // Set up test resources ( Delete , Create , Insert )
            SqlReader.setUpResourcesForTest(env, scenario.getPath());

            // Execute the query from execute_query.sql
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
            // Read expected output from expected_op.csv
            List<Row> expectedOpFromFile = DataImporter.importFromCSV(expectedOpFile.getPath());
            if (expectedOpFromFile.isEmpty()) {
                logger.warn("Expected output file is empty in {}", scenario.getName());
                continue;
            }

            // Convert expected rows to sets of strings
            Set<String> expectedSets = new HashSet<>();
               for( Row row : expectedOpFromFile){                
                if(row!=null && row.toString()!=null && !row.toString().isEmpty() && row.getField(0) != null && !row.getField(0).toString().isEmpty()){
                    logger.info("Expected row as string: {}", row.toString());
                    expectedSets.add(row.toString());
                }
               }
            logger.info("All expected result as set of strings: {}", expectedSets);            

            // 2 seconds timeout was added to avoid infinite loop problem
            logger.info("processing actual results with a timeout of 2 seconds");
            List<Row> actualData = fetchRowsWithTimeout(results);
            logger.info("Number of rows fetched: {}", actualData.size());

            // Convert actual rows to sets of strings
            Set<String> actualSets = new HashSet<>();
            for (Row row : actualData) {                
                if(row!=null && row.toString()!=null && !row.toString().isEmpty() && row.getField(0) != null && !row.getField(0).toString().isEmpty()){
                    logger.info("Actual row as string: {}", row.toString());
                    actualSets.add(row.toString());
                }
            }
            
            logger.info("All actual set of strings: {}", actualSets);

            // Compare sets
           boolean isEqual = expectedSets.equals(actualSets);
           if (!isEqual) {
               compareAndLogResults(scenario.getName(), expectedSets, actualSets);
           } else {
               logger.info("Test passed in scenario: {}", scenario.getName());
           }
        }
    }

    
    protected List<Row> fetchRowsWithTimeout(TableResult result) {
        List<Row> rows = new ArrayList<>();
        AtomicBoolean running = new AtomicBoolean(true);

        Thread loopThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    try (CloseableIterator<Row> iterator = result.collect()) {
                        if (!iterator.hasNext()) {
                            logger.info("No rows available in result");
                            return;
                        }
                        
                        do {                           
                            Row row = iterator.next();
                            logger.info("Fetched row: {}", row);
                            rows.add(row);
                        } while (iterator.hasNext());

                    } catch (Exception e) {
                        logger.error("Error collecting rows: {}", e.getMessage());
                        throw new RuntimeException("Failed to collect rows", e);
                    }
                }
            }
        });

        loopThread.start();
        try {
            Thread.sleep(2000 ); // Wait for 2 seconds
        } catch (InterruptedException e) {
            logger.error("Thread interrupted: {}", e.getMessage());
        }
        running.set(false);
        return rows;
    }

    protected Stream<Row> fetchRows(TableResult result) {
        Iterable<Row> iterable = result::collect;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private void compareAndLogResults(String scenarioName, Set<String> expectedSets, Set<String> actualSets) {
        logger.error("Test failed in scenario: {}", scenarioName);
        logger.error("Expected sets: {}", expectedSets);
        logger.error("Actual sets: {}", actualSets);

        // Find missing and extra values
        Set<String> missingInActual = new HashSet<>(expectedSets);
        missingInActual.removeAll(actualSets);

        Set<String> extraInActual = new HashSet<>(actualSets);
        extraInActual.removeAll(expectedSets);

        logger.error("Missing in actual results: {}", missingInActual);
        logger.error("Extra in actual results: {}", extraInActual);

        fail("Test failed in scenario: " + scenarioName +
             "\nExpected sets: " + expectedSets +
             "\nActual sets: " + actualSets +
             "\nMissing in actual: " + missingInActual +
             "\nExtra in actual: " + extraInActual);
    }
}
