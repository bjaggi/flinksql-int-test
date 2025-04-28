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
        
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            // Read header with data types
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("Empty CSV file");
            }
            
            // Read data rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                Object[] rowData = new Object[values.length];
                
                for (int i = 0; i < values.length; i++) {
                    String value = values[i].trim();
                    if (value.isEmpty()) {
                        rowData[i] = null;
                    } else {
                        // Convert value based on type from header
                        String type = headerLine.split(",")[i].trim();
                        rowData[i] = convertValue(value, type);
                    }
                }
                
                rows.add(Row.of(rowData));
            }
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