package io.confluent.flink.examples.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CSVFileReader {
    private static final Logger logger = LoggerFactory.getLogger(CSVFileReader.class);

    public static List<String[]> readCSV(String filePath) {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                data.add(values);
            }
        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", filePath, e);
        }
        return data;
    }

    public static void printCSVData(List<String[]> data) {
        for (String[] row : data) {
            logger.info("{}", String.join(", ", row));
        }
    }
}


