package io.confluent.flink.examples.helper;

import com.opencsv.CSVReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class OpenCSVReader {
    public static void main(String[] args) {
        String csvFile = "path/to/your/file.csv";

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                System.out.println(Arrays.toString(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


