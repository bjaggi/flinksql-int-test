
CREATE TABLE `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` (
  `key` VARCHAR(2147483647) NOT NULL,
  `preprocessWindow` VARCHAR(2147483647) COMMENT 'Gets or sets the window to preprocess',
  `window` VARCHAR(2147483647) COMMENT 'Gets or sets the window to release'
)
DISTRIBUTED BY HASH(`key`) INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'confluent',
  'kafka.cleanup-policy' = 'delete',
  'kafka.max-message-size' = '2097164 bytes',
  'kafka.retention.size' = '0 bytes',
  'kafka.retention.time' = '7 d',
  'key.format' = 'json-registry',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'json-registry'
);
	