package io.confluent.flink.examples.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SqlReader {
    private static final Logger logger = LoggerFactory.getLogger(SqlReader.class);

    public static TableInfo readTableDefinitionAndData(String csvFile) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // Read table name (first line)
            String tableName = br.readLine().trim();
            
            // Read column names (second line)
            String[] columnNames = br.readLine().split(",");
            
            // Read data types (third line)
            String[] dataTypes = br.readLine().split(",");
            
            // Read and store the data
            List<String[]> data = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line.split(","));
            }
            
            return new TableInfo(tableName, 
                            Arrays.asList(columnNames), 
                            Arrays.asList(dataTypes), 
                            data);
        }
    }

    private static void executeSql(String sql, TableEnvironment env) {
        if (sql == null || sql.trim().isEmpty()) {
            return;
        }
        try {
            env.executeSql(sql).await();
            logger.info("Successfully executed SQL");
        } catch (Exception e) {
            logger.error("Error executing SQL: {}", e.getMessage(), e);
        }
    }

    protected void deleteTables( TableEnvironment env) {
    
            try {
                env.executeSql(TestConstants.DROP_TABLES_SQL).await();
            } catch (Exception e) {
                logger.error("Unable to delete table/s: ",  e);
            }
        //}
    }

    private static void insertData(File file, TableEnvironment env) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            StringBuilder sqlBuilder = new StringBuilder();
            String line;
            
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    sqlBuilder.append(line).append("\n");
                }
            }
            
            String sql = sqlBuilder.toString();
            executeSql(sql, env);
        } catch (IOException e) {
            logger.error("Error reading file: {}", file.getPath(), e);
        }
    }

    private static void createTable(File file) {
        // Implementation needed
    }

    private static String getResourcesPath() {
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

    public static void listResources(TableEnvironment env) {
        String resourcesPath = getResourcesPath();
        File resourcesDir = new File(resourcesPath);
        
        if (resourcesDir.exists() && resourcesDir.isDirectory()) {
            File[] folders = resourcesDir.listFiles(File::isDirectory);
            logger.info("Found {} folders in the resources directory.", folders.length);
            for (File folder : folders) {
                logger.info("{}", folder);
            }
            logger.info("\n\n");
            
            if (folders != null) {
                for (File folder : folders) {
                    logger.info("Starting to execute tests in Folder: {}", folder.getName());
                    logger.info("");
                    File[] files = folder.listFiles(File::isFile);
                    if (files != null) {
                        for (File file : files) {
                            logger.info("Found file: {}", file.getName());
                            if (file.getName().contains(String.valueOf("insert_data").toLowerCase())) {                                    
                                logger.info("  Inserting data from file: {}", file.getName());
                                insertData(file, env);
                            } else if (file.getName().contains(String.valueOf(".sql").toLowerCase())) {
                                logger.info("  Executing the query present in the file: {}", file.getName());
                                createTable(file);
                            } else {
                                logger.info("  Skipping file: {}", file.getName());
                            }
                            logger.info("");
                        }
                    }
                }
            }
        }
    }

    public static List<String> readSqlFiles(String directoryPath) {
        List<String> sqlContents = new ArrayList<>();
        try {
            File directory = new File(directoryPath);
            if (!directory.exists() || !directory.isDirectory()) {
                logger.error("Invalid directory path: {}", directoryPath);
                return sqlContents;
            }

            File[] folders = directory.listFiles(File::isDirectory);
            if (folders != null) {
                for (File folder : folders) {
                    logger.info("Processing folder: {}", folder.getName());
                    Path folderPath = Paths.get(folder.getPath());
                    Files.walk(folderPath)
                            .filter(Files::isRegularFile)
                            .filter(path -> path.toString().endsWith(".sql"))
                            .forEach(path -> {
                                try {
                                    String content = Files.readString(path);
                                    sqlContents.add(content);
                                    logger.info("Read SQL file: {}", path.getFileName());
                                } catch (IOException e) {
                                    logger.error("Error reading SQL file: {}", path, e);
                                }
                            });
                }
            }
        } catch (IOException e) {
            logger.error("Error processing directory: {}", directoryPath, e);
        }
        return sqlContents;
    }
}
