package io.confluent.flink.examples.helper;

import org.apache.flink.types.Row;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataExporter {
    private static final Logger logger = LoggerFactory.getLogger(DataExporter.class);
    
    /**
     * Exports Row data to a CSV file with data types
     * @param data List of Row objects to export
     * @param filePath Path to the output CSV file
     */
    public static void exportToCSV(List<Row> data, String filePath) throws IOException {
        if (data == null || data.isEmpty()) {
            throw new IllegalArgumentException("Data list cannot be null or empty");
        }

        try (FileWriter writer = new FileWriter(filePath)) {
            // Write header with data types
            Row firstRow = data.get(0);
            StringBuilder header = new StringBuilder();
            for (int i = 0; i < firstRow.getArity(); i++) {
                Object value = firstRow.getField(i);
                String type = value != null ? value.getClass().getSimpleName() : "NULL";
                header.append(type);
                if (i < firstRow.getArity() - 1) {
                    header.append(",");
                }
            }
            writer.write(header.toString() + "\n");

            // Write data rows
            for (Row row : data) {
                StringBuilder rowData = new StringBuilder();
                for (int i = 0; i < row.getArity(); i++) {
                    Object value = row.getField(i);
                    rowData.append(value != null ? value.toString() : "");
                    if (i < row.getArity() - 1) {
                        rowData.append(",");
                    }
                }
                writer.write(rowData.toString() + "\n");
            }
            logger.info("Successfully exported {} rows to {}", data.size(), filePath);
        } catch (IOException e) {
            logger.error("Failed to export data to {}: {}", filePath, e.getMessage(), e);
            throw e;
        }
    }
} 