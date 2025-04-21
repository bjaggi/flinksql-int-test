package io.confluent.flink.examples;

import io.confluent.flink.examples.helper.SqlReader;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import io.confluent.flink.plugin.ConfluentSettings;

public class Main {
    public static void main(String[] args) {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment env = TableEnvironment.create(settings);

        System.out.println("Starting Flink Integration Tests!");
        System.out.println();System.out.println();
        SqlReader.listResources(env);
    }

    // private static void loadSqlFromFile(String path) {
    //     //SqlReader.
    // }
}