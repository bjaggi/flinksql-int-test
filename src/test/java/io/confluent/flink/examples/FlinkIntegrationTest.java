package io.confluent.flink.examples;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import io.confluent.flink.plugin.ConfluentSettings;

public abstract class FlinkIntegrationTest {
    protected TableEnvironment env;

    public abstract void setup();
} 