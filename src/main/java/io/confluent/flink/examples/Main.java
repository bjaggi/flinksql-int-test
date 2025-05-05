package io.confluent.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.confluent.flink.examples.helper.SqlReader;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting Flink Integration Tests");
        // EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        //TableEnvironment env = TableEnvironment.create(settings);
        //SqlReader.listResources(env);
    }

    // private static void loadSqlFromFile(String path) {
    //     //SqlReader.
    // }
}