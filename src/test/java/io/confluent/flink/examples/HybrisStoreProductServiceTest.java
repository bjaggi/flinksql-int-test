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
import io.confluent.flink.examples.helper.RowComparator;
import io.confluent.flink.examples.helper.DataExporter;
import io.confluent.flink.examples.helper.DataImporter;
import io.confluent.flink.examples.helper.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;
import java.io.IOException;
import java.nio.file.*;
import java.io.File;

import static io.confluent.flink.examples.HybrisStoreProductService.hybrisStoreProductTableName;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
public class HybrisStoreProductServiceTest extends FlinkIntegrationTest {
    private static final Logger logger = LoggerFactory.getLogger(HybrisStoreProductServiceTest.class);
    private static final String breakline = "------------------------------------------------------------------------------------------------------------------------";
    private HybrisStoreProductService hybrisStoreProductService;

    @Override
    public void setup() {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment env = TableEnvironment.create(settings);

        logger.info("Starting Flink Integration Tests!");
        logger.info("Initializing test environment...");
        SqlReader.listResources(env);
        deleteTable(TestConstants.PRODUCTS_ELIGIBILITY_TABLE, env);
        hybrisStoreProductService = new HybrisStoreProductService(
                env,
                hybrisStoreProductTableName, hybrisStoreProductTableName
        );
    }

    @Test
    @Timeout(240)
    public void hybrisQueryTest() throws Exception {
        // Read expected data from CSV files
        List<Row> expectedOpFromFile = new ArrayList<>();
        String basePath = "src/main/resources/execute_tests";
        File baseDir = new File(basePath);
        logger.info("Base directory path: {}", baseDir.getAbsolutePath());
        logger.info("Base directory exists: {}", baseDir.exists());
        logger.info("Base directory is directory: {}", baseDir.isDirectory());

        if (baseDir.exists() && baseDir.isDirectory()) {
            File[] subDirs = baseDir.listFiles(File::isDirectory);
            logger.info("Number of subdirectories found: {}", (subDirs != null ? subDirs.length : 0));
            
            if (subDirs != null) {
                for (File subDir : subDirs) {
                    logger.info("\n\n" + breakline);    
                    logger.info("\n*** Processing all tests in the subdirectory *** : {}", subDir.getName());
                    try {
                        String csvPath = subDir.getPath() + File.separator + TestConstants.EXPECTED_OUTPUT_CSV;
                        logger.info("Reading CSV file from: {}", csvPath);
                        
                        File expectedOpFile = new File(csvPath);
                        if (expectedOpFile.exists()) {
                            List<Row> rows = DataImporter.importFromCSV(csvPath);
                            expectedOpFromFile.addAll(rows);
                            logger.info("Read {} rows from {}", rows.size(), csvPath);
                        } else {
                            logger.error("Expected output file not found: {}", csvPath);
                        }
                    } catch (IOException e) {
                        logger.error("Failed to read expected data from {}: {}", subDir.getName(), e.getMessage(), e);
                    }





                     // Execute the query.
        TableResult results = hybrisStoreProductService.executeHybrisStoreProductQuery();
        logger.info("Table API job id: {}", results.getJobClient().stream().toList());

        // Fetch the actual results.
        List<Row> actualData = fetchRows(results)
                .limit(expectedOpFromFile.size())
                .toList();

        // Compare results using the new comparator
        for (int i = 0; i < expectedOpFromFile.size(); i++) {
            logger.info("\nComparing Row {}:", i);
            logger.info("Expected: {}", expectedOpFromFile.get(i));
            logger.info("Actual:   {}", actualData.get(i));
            
            RowComparator.ComparisonResult comparison = RowComparator.compareRows(expectedOpFromFile.get(i), actualData.get(i));
            
            boolean assertionResult = comparison.isEqual();
            logger.info("Assertion Result: {}", (assertionResult ? "PASSED" : "FAILED"));
            if (!assertionResult) {
                logger.error("Failure Message: {}", comparison.getMessage());
            }
            
            assertTrue(assertionResult, "Row " + i + " comparison failed: " + comparison.getMessage());
        }
                }
            }
        } else {
            logger.error("Base directory does not exist or is not a directory: {}", basePath);
        }

       
    }

    protected void deleteTable(String tableName, TableEnvironment env) {
        String[] tablePath = tableName.split("\\.");

        String catalog = tablePath[0].replace("`", "");
        String database = tablePath[1].replace("`", "");
        String table = tablePath[2].replace("`", "");

        if(Arrays.asList(env.listTables(catalog, database)).contains(table)) {
            logger.info("Deleting table {}", tableName);

            try {
                env.executeSql(String.format("DROP TABLE %s", tableName)).await();
            } catch (Exception e) {
                logger.error("Unable to delete temporary table: {}", tableName, e);
            }
        }
    }
}
