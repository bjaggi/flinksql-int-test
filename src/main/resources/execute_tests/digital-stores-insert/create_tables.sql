-- Create tables for digital stores insert test
CREATE TABLE IF NOT EXISTS `${DIGITAL_STORES_TABLE}` (
    `store_id` STRING,
    `store_name` STRING,
    `store_type` STRING,
    `is_active` BOOLEAN,
    `created_at` TIMESTAMP,
    `updated_at` TIMESTAMP
) WITH (
    'connector' = '${KAFKA_CONNECTOR}',
    'topic' = '${DIGITAL_STORES_TOPIC}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = '${KAFKA_GROUP_ID}',
    'format' = '${KAFKA_FORMAT}',
    'scan.startup.mode' = '${KAFKA_STARTUP_MODE}'
);

CREATE TABLE `Development`.`Digital-Public-Development`.`shared.digital.products.eligibility` (
  `isEligible`