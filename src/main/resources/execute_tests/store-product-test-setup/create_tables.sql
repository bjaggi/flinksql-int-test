-- Create tables for store product test setup
CREATE TABLE IF NOT EXISTS `${PRODUCTS_ELIGIBILITY_TABLE}` (
    `store_id` STRING,
    `product_id` STRING,
    `is_eligible` BOOLEAN,
    `last_updated` TIMESTAMP
) WITH (
    'connector' = '${KAFKA_CONNECTOR}',
    'topic' = '${PRODUCTS_ELIGIBILITY_TOPIC}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = '${KAFKA_GROUP_ID}',
    'format' = '${KAFKA_FORMAT}',
    'scan.startup.mode' = '${KAFKA_STARTUP_MODE}'
);
