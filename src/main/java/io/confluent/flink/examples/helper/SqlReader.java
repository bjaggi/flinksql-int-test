package io.confluent.flink.examples.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import io.confluent.flink.examples.helper.TestConstants.*;
import org.apache.flink.table.api.TableResult;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
            
            // Validate column count matches between names and types
            if (columnNames.length != dataTypes.length) {
                throw new IllegalArgumentException(
                    String.format("Column count mismatch: %d column names but %d data types in file %s",
                        columnNames.length, dataTypes.length, csvFile));
            }
            
            // Validate each data type ( this has issues with comments)
            // for (int i = 0; i < dataTypes.length; i++) {
            //     String dataType = dataTypes[i].trim().toUpperCase();
            //     if (!isValidDataType(dataType)) {
            //         throw new IllegalArgumentException(
            //             String.format("Invalid data type '%s' for column '%s' in file %s",
            //                 dataType, columnNames[i], csvFile));
            //     }
            // }
            
            // Read and store the data
            List<String[]> data = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                String[] row = line.split(",");
                // Validate each row has correct number of columns
                if (row.length != columnNames.length) {
                    throw new IllegalArgumentException(
                        String.format("Row has %d columns but expected %d columns in file %s",
                            row.length, columnNames.length, csvFile));
                }
                data.add(row);
            }
            
            logger.info("Successfully validated CSV file {} with {} columns and {} rows",
                csvFile, columnNames.length, data.size());
            
            return new TableInfo(tableName, 
                            Arrays.asList(columnNames), 
                            Arrays.asList(dataTypes), 
                            data);
        }
    }

    private static TableResult executeSql(String sql, TableEnvironment env) {
        if (sql == null || sql.trim().isEmpty()) {
            return null;
        }
        try {
            return env.executeSql(sql);
        } catch (Exception e) {
            logger.error("Error executing SQL: {}", e.getMessage(), e);
            return null;
        }
    }

    private static String readSqlFromFile(File file) throws IOException {
        StringBuilder sqlBuilder = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    sqlBuilder.append(line).append("\n");
                }
            }
        }
        return sqlBuilder.toString();
    }

    protected static void deleteTables(File file, TableEnvironment env) {
        try {
            File[] files = file.listFiles(File::isFile);

            if (files != null) {
                logger.info("Found {} SQL files to execute in {} folder", files.length, file.getName());
                for (File sqlFile : files) {
                    logger.info("Executing SQL to Drop tables from file: {}", sqlFile.getName());
                    String sql = readSqlFromFile(sqlFile);
                      TableResult result = executeSql(sql, env);
                      if (result != null) {
                        try {
                            result.await(90, TimeUnit.SECONDS);
                            logger.info("    Successfully dropped tables from file: {}", sqlFile.getName());
                        } catch (TimeoutException e) {
                            logger.error("    Table drop timed out after 90 seconds for file: {}", sqlFile.getName());
                            throw new RuntimeException("Table creation timed out", e);
                        } catch (InterruptedException | ExecutionException e) {
                            logger.error("    Error waiting for table to be dropped: {}", e.getMessage());
                            throw new RuntimeException("Error waiting for table deletion", e);
                        }catch (Exception e) {
                            logger.error("    Error waiting for table drop: {}", e.getMessage());
                            throw new RuntimeException("Error waiting for table creation", e);
                        }
                    }

                      logger.info("    Sucessfully Dropped tables mentioned in the file: {}", sqlFile.getName());
                }
            } else {
                logger.info("No SQL files found in {} folder");
            }
        } 
        catch (Exception e) {
            logger.error("Unable to delete table/s: ", e);
        }
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
            logger.info("Executing insert from file: {}", file.getName());
            TableResult result = executeSql(sql, env);
            if (result != null) {
                try {
                    result.await(90, TimeUnit.SECONDS);
                    logger.info("Successfully inserted data from file: {}", file.getName());
                } catch (TimeoutException e) {
                    logger.error("Data insertion timed out after 90 seconds for file: {}", file.getName());
                    throw new RuntimeException("Data insertion timed out", e);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Error waiting for data insertion: {}", e.getMessage());
                    throw new RuntimeException("Error waiting for data insertion", e);
                }
            }
        } catch (IOException e) {
            logger.error("Error reading file: {}", file.getPath(), e);
            throw new RuntimeException("Error reading SQL file for data insertion", e);
        }
    }

    public static TableResult executeQuery(File file, TableEnvironment env) {
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            StringBuilder sqlBuilder = new StringBuilder();
            String line;
            
            while ((line = br.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    sqlBuilder.append(line).append("\n");
                }
            }
            
            String sql = sqlBuilder.toString();
            logger.info("Executing query from file: {}", file.getName());
            TableResult result = executeSql(sql, env);
            if (result != null) {
                try {
                    // Increased timeout to 5 minutes
                    result.await(90, TimeUnit.SECONDS);
                    
                } catch (TimeoutException e) {
                    logger.error("Query execution timed out after 90 secs for file: {}", file.getName());
                    throw new RuntimeException("Query execution timed out", e);
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Error waiting for query completion: {}", e.getMessage());
                    throw new RuntimeException("Error waiting for query completion", e);
                }
                logger.info("Query execution completed successfully");
            }
            return result;
        } catch (IOException e) {
            logger.error("Error reading file: {}", file.getPath(), e);
            throw new RuntimeException("Error reading SQL file", e);
        }
    }

    private static void validateSqlSchema(String sql) {
        // Extract column definitions from CREATE TABLE statement
        Pattern createTablePattern = Pattern.compile("CREATE\\s+TABLE\\s+[^(]*\\((.*)\\)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        java.util.regex.Matcher matcher = createTablePattern.matcher(sql);
        
        if (matcher.find()) {
            String columnDefinitions = matcher.group(1);
            String[] columns = columnDefinitions.split(",");
            
            // Count columns and validate data types
            int columnCount = 0;
            for (String column : columns) {
                column = column.trim();
                if (!column.isEmpty() && !column.startsWith("PRIMARY KEY") && !column.startsWith("CONSTRAINT")) {
                    columnCount++;
                    // Split by whitespace but keep quoted identifiers together
                    List<String> parts = new ArrayList<>();
                    Pattern pattern = Pattern.compile("([^\\s]+)|'([^']*)'|`([^`]*)`");
                    java.util.regex.Matcher partMatcher = pattern.matcher(column);
                    while (partMatcher.find()) {
                        String part = partMatcher.group();
                        if (part != null && !part.isEmpty()) {
                            parts.add(part);
                        }
                    }
                    
                    if (parts.size() < 2) {
                        throw new IllegalArgumentException("Invalid column definition: " + column);
                    }
                    
                    // Find the data type part (it's the first word after the column name)
                    String dataType = null;
                    for (int i = 1; i < parts.size(); i++) {
                        String part = parts.get(i).toUpperCase();
                        if (isValidDataType(part)) {
                            dataType = part;
                            break;
                        }
                    }
                    
                    if (dataType == null) {
                        throw new IllegalArgumentException("No valid data type found in column definition: " + column);
                    }
                }
            }
            
            if (columnCount == 0) {
                throw new IllegalArgumentException("No columns defined in CREATE TABLE statement");
            }
            
            logger.info("Validated SQL schema with {} columns", columnCount);
        } else {
            throw new IllegalArgumentException("Invalid CREATE TABLE statement format");
        }
    }

    private static boolean isValidDataType(String dataType) {
        // List of valid Flink SQL data types
        String[] validTypes = {
            "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL",
            "VARCHAR", "CHAR", "STRING", "BINARY", "VARBINARY", "BYTES",
            "DATE", "TIME", "TIMESTAMP", "TIMESTAMP_LTZ",
            "ARRAY", "MAP", "MULTISET", "ROW",
            "RAW", "NULL"
        };
        
        // Check if the data type starts with any valid type
        for (String type : validTypes) {
            if (dataType.startsWith(type)) {
                return true;
            }
        }
        return false;
    }

    private static void createTable(File subDirectory, TableEnvironment env) {
        String sql = "";
        try {
            File[] files = subDirectory.listFiles(File::isFile);
            
            if (files != null) {
                logger.info("Found {} SQL files to execute in {} folder", files.length, subDirectory.getName());
                for (File sqlFile : files) {
                    logger.info("Executing SQL from file: {}", subDirectory.getName()+File.separator+sqlFile.getName());
                    sql = readSqlFromFile(sqlFile);
                    TableResult result = executeSql(sql, env);
                    if (result != null) {
                        try {
                            result.await(90, TimeUnit.SECONDS);
                            logger.info("Successfully created tables from file: {}", sqlFile.getName());
                        } catch (TimeoutException e) {
                            logger.error("Table creation timed out after 90 seconds for file: {}", sqlFile.getName());
                            throw new RuntimeException("Table creation timed out", e);
                        } catch (InterruptedException | ExecutionException e) {
                            logger.error("Error waiting for table creation: {}", e.getMessage());
                            throw new RuntimeException("Error waiting for table creation", e);
                        }
                    }
                }
            } else {
                logger.info("No SQL files found in {} folder");
            }
        } catch (Exception e) {
            logger.error("Unable to create table/s using the query:{} ", sql, e);
            throw new RuntimeException("Failed to create tables", e);
        }
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

    public static void setUpResourcesForTest(TableEnvironment env, String testFolderPathString) {
        File resourcesDir = new File(testFolderPathString);
        
        if (resourcesDir.exists() && resourcesDir.isDirectory()) {
            File[] files = resourcesDir.listFiles(File::isFile);
            File[] subDirectories = resourcesDir.listFiles(File::isDirectory);
            if (files != null) {
                
                // First process delete files
                for (File subDirectory : subDirectories) {
                    if (subDirectory.getName().equalsIgnoreCase(TestConstants.DROP_TABLES_DIRECTORY.toLowerCase())) {
                        logger.info(" Executing to delete all tables in the {} ", subDirectory.getName());
                        deleteTables(subDirectory, env);
                    }
                // Then process create files
                    if (subDirectory.getName().equalsIgnoreCase(TestConstants.CREATE_TABLES_DIRECTORY.toLowerCase())) {
                        logger.info("Found subdirectory : {}", subDirectory.getName());
                        logger.info(" Will start executing all SQL files in the {} folder to Create all tables ", subDirectory.getName());
                        createTable(subDirectory, env);
                    }

                }
                                                  
                // Finally process insert files
                for (File file : files) {
                    if (file.getName().contains(String.valueOf(TestConstants.INSERT_DATA_SQL).toLowerCase())) {
                        logger.info("Found File : {}", file.getName());
                        logger.info(" Executing {} to Insert data into all tables ", file.getName());
                        insertData(file, env);
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

    public static void printDirectoryStructure(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            logger.error("Invalid directory path: {}", directoryPath);
            return;
        }

        logger.info("\nDirectory Structure for: {}", directoryPath);
        printDirectoryContents(directory, 0);
    }

    private static void printDirectoryContents(File directory, int level) {
        String indent = "  ".repeat(level);
        File[] files = directory.listFiles();
        
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    logger.info("{}📁 {}", indent, file.getName());
                    printDirectoryContents(file, level + 1);
                } else {
                    logger.info("{}📄 {}", indent, file.getName());
                }
            }
        }
    }
}
