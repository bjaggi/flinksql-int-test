

CREATE TABLE `Development`.`Digital-Public-Development`.`shared.digital.products.eligibility` (
  `isEligible` BOOLEAN COMMENT 'Gets or sets a value indicating whether the product is eligible to be sold online.',
  `productId` VARCHAR COMMENT 'Gets or sets the Meijer Product Identification.',
  `storeId` BIGINT NOT NULL COMMENT 'Gets or sets the identifier for the store.',
  `upcId` VARCHAR NOT NULL COMMENT 'Gets or sets the UPC ID of the item.',
  `upcTypeName` VARCHAR NOT NULL COMMENT ' ',
  `timestamp` TIMESTAMP METADATA FROM 'timestamp',
  `partition` BIGINT METADATA FROM 'partition' VIRTUAL,
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL,
  `headers` MAP<VARCHAR, VARCHAR> METADATA,
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
