-- shared-digital-stores-stores-insert


-- INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.price-current-release` (
--     key_priceId,
--     key_window,
--     priceId,
--     `window`,
--     currentPriceId,
--     currentPricePk,
--     operations,
--     headers
-- )
-- SELECT /*+ STATE_TTL('stage'='6h', 'win'='1h') */
SELECT 
    stage.`priceId` AS key_priceId,
    win.`window` AS key_window,
    stage.`priceId`,
    win.`window`,
    stage.`currentPriceId`,
    stage.`currentPricePk`,
    stage.`operations`,
    MAP[
        'source:private.digital.products.price-current-stage',
        CONCAT(
            'Partition: ', CAST(stage.`partition` AS STRING),
            ', Offset: ', CAST(stage.`offset` AS STRING),
            ', Timestamp: ', CAST(stage.`timestamp` AS STRING)
        ),
        'source:private.digital.products.process-scheduled-price-window',
        CONCAT(
            'Partition: ', CAST(win.`partition` AS STRING),
            ', Offset: ', CAST(win.`offset` AS STRING),
            ', Timestamp: ', CAST(win.`timestamp` AS STRING)
        )
    ] AS headers
FROM `Development`.`Digital-Public-Development`.`private.digital.products.price-current-stage` AS stage
INNER JOIN `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` AS win
    ON stage.`window` = win.`window`;


