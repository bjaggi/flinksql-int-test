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

               
        hybrisStoreProductService = new HybrisStoreProductService(env);
    }

    @Test
    @Timeout(240)
    //@setUpResourcesForTest()
    public void hybrisQueryTest() throws Exception {
        // Read expected data from CSV files
        List<Row> expectedOpFromFile = new ArrayList<>();
        String basePath = "src/main/resources/execute_tests";
        File baseDir = new File(basePath);
        logger.info("Base directory path: {}", baseDir.getAbsolutePath());

        if (baseDir.exists() && baseDir.isDirectory()) {
            File[] subDirs = baseDir.listFiles(File::isDirectory);
            logger.info("Found {}  📁 subdirectories under execute_tests folder   ", (subDirs != null ? subDirs.length : 0));
            
            if (subDirs != null) {
                logger.info("    All subdirectories 📁 📁  in {}:", subDirs);   
                logger.info("    Will now start processing all tests in the subdirectories 📁 📁  under {} folder", basePath);
             
                for (File dir : subDirs) {
                    logger.info("      - 📁 {}", dir.getName());
                }
                
                for (File subDir : subDirs) {
                    logger.info("\n\n" );
                    logger.info( breakline);                     
                    logger.info("Starting Flink Integration Tests for subdirectory 📁 : {}", subDir.getName());
                    logger.info( breakline);  

                    logger.info("Here is the list of all sub directories & files under {}...", subDir.getName());
                    String testFolderPathString = subDir.getPath() + File.separator;
                    SqlReader.printDirectoryStructure(testFolderPathString);
                    // This is where the tables are dropped,  created & data is inserted
                    SqlReader.setUpResourcesForTest(env, testFolderPathString);

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
                    logger.info("Executing query from file {}{}.", testFolderPathString, TestConstants.QUERY_FILE_NAME);
                    File executeSqlFile = new File(testFolderPathString, TestConstants.QUERY_FILE_NAME);
                    TableResult results = SqlReader.executeQuery(executeSqlFile, env);
                    logger.info("Table API job id: {}", results.getJobClient().stream().toList());

                    // Fetch the actual results.
                    List<Row> actualData = fetchRows(results)
                            .limit(expectedOpFromFile.size())
                            .toList();

                    // Compare results using the new comparator, excluding headers column
                    for (int i = 0; i < expectedOpFromFile.size(); i++) {
                        logger.info("\nComparing Row {}:", i);
                        Row expectedRow = expectedOpFromFile.get(i);
                        Row actualRow = actualData.get(i);
                        
                        // Get the minimum length between the two rows to avoid array bounds issues
                        int minLength = Math.min(expectedRow.getArity(), actualRow.getArity());
                        
                        // Create new rows without the headers column (last column)
                        Object[] expectedFields = new Object[minLength - 1];
                        Object[] actualFields = new Object[minLength - 1];
                        
                        // Copy all fields except the last one (headers)
                        for (int j = 0; j < minLength - 1; j++) {
                            expectedFields[j] = expectedRow.getField(j);
                            actualFields[j] = actualRow.getField(j);
                        }
                        
                        Row expectedWithoutHeaders = Row.of(expectedFields);
                        Row actualWithoutHeaders = Row.of(actualFields);
                        
                        logger.info("Expected: {}", expectedWithoutHeaders);
                        logger.info("Actual:   {}", actualWithoutHeaders);
                        
                        // Log the number of fields being compared
                        logger.info("Comparing {} fields between expected and actual rows", minLength - 1);
                        
                        RowComparator.ComparisonResult comparison = RowComparator.compareRows(expectedWithoutHeaders, actualWithoutHeaders);
                        
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


}
