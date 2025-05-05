package io.confluent.flink.examples.helper;

import org.apache.flink.types.Row;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataImporter {
    private static final Logger logger = LoggerFactory.getLogger(DataImporter.class);
    
    /**
     * Reads Row data from a CSV file with data types in the header
     * @param filePath Path to the CSV file
     * @return List of Row objects
     */
    public static List<Row> importFromCSV(String filePath) throws IOException {
        List<Row> rows = new ArrayList<>();
        logger.info("Reading from CSV : {}", filePath);
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            // Read header with data types
            String headerLine = reader.readLine();
            if (headerLine == null) {
                logger.error("Empty CSV file: {}", filePath);
                throw new IOException("Empty CSV file");
            }
            
            String[] headerTypes = headerLine.split(",");
            int expectedColumns = headerTypes.length;
            logger.info("Found {} columns in header: {}", expectedColumns, String.join(", ", headerTypes));
            
            // Read data rows
            String line;
            int rowCount = 0;
            while ((line = reader.readLine()) != null) {
                rowCount++;
                String[] values = line.split(",");
                Object[] rowData = new Object[expectedColumns];
                
                // Fill in values up to the minimum length
                int minLength = Math.min(values.length, expectedColumns);
                logger.debug("Processing row {} with {} values (expected {})", rowCount, values.length, expectedColumns);
                
                for (int i = 0; i < minLength; i++) {
                    String value = values[i].trim();
                    if (value.isEmpty()) {
                        rowData[i] = null;
                        logger.debug("Column {} is empty, setting to null", i);
                    } else {
                        rowData[i] = convertValue(value, headerTypes[i].trim());
                        logger.debug("Column {}: converted '{}' to type {}", i, value, headerTypes[i].trim());
                    }
                }
                
                // Fill remaining columns with null if data row is shorter than header
                for (int i = minLength; i < expectedColumns; i++) {
                    rowData[i] = null;
                    logger.debug("Column {}: no data, setting to null", i);
                }
                
                rows.add(Row.of(rowData));
            }
            logger.info("Successfully imported {} rows from CSV", rowCount);
        } catch (Exception e) {
            logger.error("Error importing CSV from {}: {}", filePath, e.getMessage(), e);
            throw e;
        }
        
        return rows;
    }
    
    private static Object convertValue(String value, String type) {
        try {
            switch (type) {
                case "String":
                    return value;
                case "Integer":
                    return Integer.parseInt(value);
                case "Long":
                    return Long.parseLong(value);
                case "Double":
                    return Double.parseDouble(value);
                case "Boolean":
                    return Boolean.parseBoolean(value);
                case "NULL":
                    return null;
                default:
                    return value;
            }
        } catch (NumberFormatException e) {
            logger.error("Error converting value '{}' to type {}", value, type);
            return value;
        }
    }
} 