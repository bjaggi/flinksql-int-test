package io.confluent.flink.examples.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CSVFileReader {
    public static void readCSVSkippingHeader(String csvFile) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            // Skip the first line (header)
            br.readLine();
            
            // Read remaining lines
            String line;
            while ((line = br.readLine()) != null) {
                String[] data = line.split(",");
                // Process each line here
                System.out.println(java.util.Arrays.toString(data));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


