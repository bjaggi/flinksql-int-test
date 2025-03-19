CREATE TABLE `Development`.`Digital-Public-Development`.`hybris-store-product-stock` (
  `upcId` VARCHAR(2147483647) NOT NULL,
  `storeId` BIGINT NOT NULL,
  `productId` VARCHAR(2147483647) NOT NULL,
  `upcTypeName` VARCHAR(2147483647) NOT NULL,
  `stockStatus` VARCHAR(10) NOT NULL,
  `stockStatusId` BIGINT NOT NULL,
  `storeBOH` DOUBLE,
  `ilcPrimary` VARCHAR(2147483647),
  `ilcs` VARCHAR(2147483647),
  `isNewIlc` BOOLEAN NOT NULL,
  `isEligible` BOOLEAN NOT NULL,
  `isInStoreOnly` BOOLEAN,
  `headers` MAP<VARCHAR(2147483647) NOT NULL, VARCHAR(2147483647)> NOT NULL
)
DISTRIBUTED INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'retract',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.max-message-size' = '0 bytes',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'avro-registry'
);