package io.confluent.flink.examples.helper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SQLFileReader {
    private static final Logger logger = LoggerFactory.getLogger(SQLFileReader.class);

    public static String readSQLFile(String filePath) {
        StringBuilder sqlContent = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                sqlContent.append(line).append("\n");
            }
        } catch (IOException e) {
            logger.error("Error reading SQL file: {}", filePath, e);
        }
        return sqlContent.toString();
    }

    public static void printSQLContent(String sqlContent) {
        logger.info("SQL Content:\n{}", sqlContent);
    }
} 