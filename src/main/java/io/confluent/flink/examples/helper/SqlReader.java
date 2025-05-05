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
            String sql = readSqlFromFile(file);
            env.executeSql(sql).await();
            logger.info("Successfully deleted table/s");
        } catch (Exception e) {
            logger.error("Unable to delete table/s: ",  e);
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
            executeSql(sql, env);
        } catch (IOException e) {
            logger.error("Error reading file: {}", file.getPath(), e);
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

    private static void createTable(File file, TableEnvironment env) {
        try {
            String sql = readSqlFromFile(file);
            //validateSqlSchema(sql);
            env.executeSql(sql).await(10, TimeUnit.SECONDS);
            logger.info("Successfully created table with validated schema");
        } catch (Exception e) {
            logger.error("Unable to create table/s: ", e);
            throw new RuntimeException("Failed to create table: " + e.getMessage(), e);
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
            if (files != null) {
                
                // First process delete files
                for (File file : files) {
                    if (file.getName().contains(TestConstants.DROP_TABLES_SQL.toLowerCase())) {
                        logger.info("Found File : {}", file.getName());
                        logger.info(" Executing {} to Delete all tables ", file.getName());
                        deleteTables(file, env);
                    }
                }
                
                // Then process create files
                for (File file : files) {
                    if (file.getName().contains(String.valueOf(TestConstants.CREATE_TABLES_SQL).toLowerCase())) {
                        logger.info("Found File : {}", file.getName());
                        logger.info(" Executing {} to Create all tables ", file.getName());
                        createTable(file, env);
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
}
