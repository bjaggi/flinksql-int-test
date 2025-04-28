package io.confluent.flink.examples.helper;

public class TestConstants {
    // File names
    public static final String EXPECTED_OUTPUT_CSV = "expected_op.csv";
    public static final String CREATE_TABLES_SQL = "create_tables.sql";
    public static final String DROP_TABLES_SQL = "drop_tables.sql";
    public static final String INSERT_DATA_SQL = "insert_data.sql";

    // Table names
    public static final String PRODUCTS_ELIGIBILITY_TABLE = "Development.Digital-Public-Development.shared.digital.products.eligibility";
    public static final String STORES_LOCATION_TABLE = "Development.Digital-Public-Development.shared.digital.stores.location";
    public static final String DIGITAL_STORES_TABLE = "Development.Digital-Public-Development.shared.digital.stores";

    // Kafka settings
    public static final String KAFKA_CONNECTOR = "kafka";
    public static final String KAFKA_GROUP_ID = "flink-sql-test";
    public static final String KAFKA_FORMAT = "json";
    public static final String KAFKA_STARTUP_MODE = "earliest-offset";

    // Topics
    public static final String PRODUCTS_ELIGIBILITY_TOPIC = "digital.products.eligibility";
    public static final String STORES_LOCATION_TOPIC = "digital.stores.location";
    public static final String DIGITAL_STORES_TOPIC = "digital.stores";
} 