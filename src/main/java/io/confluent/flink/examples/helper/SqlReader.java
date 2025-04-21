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

public class SqlReader {
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
         // Push the customers into the temporary table.
         //env.fromValues(customers).insertInto(customersTableName).execute();
        try {
            
            env.executeSql(sql).await();
            System.out.println("Successfully executed SQL " );
        } catch (Exception e) {
            System.err.println("Error executing SQL: " + e.getMessage());
            e.printStackTrace();
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
            System.err.println("Error reading file: " + file.getPath());
            e.printStackTrace();
        }
    }

    private static void createTable(File file) {
        // Implementation for create table
    }

    public static void listResources(TableEnvironment env) {
        String resourcesPath = "src/main/resources/execute_tests";
        File resourcesDir = new File(resourcesPath);
        
        if (resourcesDir.exists() && resourcesDir.isDirectory()) {
            File[] folders = resourcesDir.listFiles(File::isDirectory);
            System.out.println("Found " + folders.length + " folders in the resources directory.");
            for (File folder : folders) {
                System.out.println(folder);
            }
            System.out.println();
            System.out.println();
            System.out.println();
            if (folders != null) {
                for (File folder : folders) {
                    
                    System.out.println("Starting to execute tests in Folder : " + folder.getName() );        
                     System.out.println();                   
                    File[] files = folder.listFiles(File::isFile);
                    if (files != null) {
                        for (File file : files) {                           
                            System.out.println("Found file: " + file.getName());
                            if (file.getName().contains(String.valueOf("insert_data").toLowerCase())) {                                    
                                System.out.println("  Inserting data from file: " + file.getName());
                                insertData(file, env);
                            } else if (file.getName().contains(String.valueOf(".sql").toLowerCase())) {
                                System.out.println("  Executing the query present in the file: " + file.getName());
                                createTable(file);
                            } else {
                                System.out.println("  Skipping file: " + file.getName());
                            }
                            System.out.println();
                        }
                    }
                }
            }
        }
    }
}
