CREATE TABLE `Development`.`Digital-Public-Development`.`shared.digital.products.eligibility` (
  `isEligible` BOOLEAN COMMENT 'Gets or sets a value indicating whether the product is eligible to be sold online.',
  `productId` VARCHAR(2147483647) COMMENT 'Gets or sets the Meijer Product Identification.',
  `storeId` BIGINT NOT NULL COMMENT 'Gets or sets the identifier for the store.',
  `upcId` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the UPC ID of the item.',
  `upcTypeName` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the UPC type applicable for store, gas station, or pharmacy.',
  `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
  `partition` BIGINT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
  `headers` MAP<VARCHAR(2147483647), VARCHAR(2147483647)> METADATA,
  CONSTRAINT `PRIMARY` PRIMARY KEY (`storeId`, `upcId`, `upcTypeName`) NOT ENFORCED
)
DISTRIBUTED BY HASH(`storeId`, `upcId`, `upcTypeName`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'compact',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '0 ms',
  'key.format' = 'json-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all',
  'value.format' = 'json-registry'
);

-- Create tables for store eligibility location test
CREATE TABLE IF NOT EXISTS `${STORES_LOCATION_TABLE}` (
    `store_id` STRING,
    `store_name` STRING,
    `latitude` DOUBLE,
    `longitude` DOUBLE,
    `address` STRING,
    `last_updated` TIMESTAMP
) WITH (
    'connector' = '${KAFKA_CONNECTOR}',
    'topic' = '${STORES_LOCATION_TOPIC}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id' = '${KAFKA_GROUP_ID}',
    'format' = '${KAFKA_FORMAT}',
    'scan.startup.mode' = '${KAFKA_STARTUP_MODE}'
);
