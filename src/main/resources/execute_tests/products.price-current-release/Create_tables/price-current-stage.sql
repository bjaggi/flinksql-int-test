



CREATE TABLE `Development`.`Digital-Public-Development`.`private.digital.products.price-current-stage` (
 `key_priceId` VARCHAR(2147483647),
  `key_window` VARCHAR(2147483647),
  `currentPriceId` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the id of the current price document
that will be updated by this change.',
  `currentPricePk` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the partition key of the Cosmos document
that will be updated by this change.',
  `operations` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the operations to be performed on the price.',
  `priceId` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the id of the price.',
  `window` VARCHAR(2147483647) NOT NULL COMMENT 'Gets or sets the time window that the price becomes
active.',
  `timestamp` TIMESTAMP(3) METADATA FROM 'timestamp',
  `partition` BIGINT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
  `headers` MAP<VARCHAR(2147483647), VARCHAR(2147483647)> METADATA
)
DISTRIBUTED BY HASH(`key_priceId`, `key_window`) INTO 20 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '14 d',
  'key.fields-prefix' = 'key_',
  'key.format' = 'json-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json-registry'
);