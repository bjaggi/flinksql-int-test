INSERT INTO `shared.digital.products.stock-eligibility-location` (
    upcId,
    storeId,
    upcTypeName,
    productId,
    stockStatus,
    stockStatusId,
    storeBOH,
    ilcPrimary,
    ilcs,
    isNewIlc,
    isEligible,
    isInStoreOnly,
    headers
)
(
    SELECT
        stock.upcId,
        stock.storeId,
        stock.upcTypeName,
        stock.productId,
        stock.stockStatus,
        stock.isSellableFlag AS stockStatusId,
        stock.storeBalanceOnHandDecimalQuantity AS storeBOH,
        loc.ilcPrimary,
        CASE
            WHEN loc.ilcs IS NULL THEN NULL
            ELSE ARRAY_JOIN(loc.ilcs, ', ')
        END AS ilcs,
        true AS isNewIlc,
        elig.isEligible,
        CASE
            WHEN info.isInStoreOnly IS NULL THEN false
            ELSE info.isInStoreOnly
        END AS isInStoreOnly,
        -- Create headers with source info for the new message
        MAP[
            'source:shared.digital.products.stock',
            CONCAT(
                'Partition: ', CAST(stock.`partition` AS STRING),
                ', Offset: ', CAST(stock.`offset` AS STRING),
                ', Timestamp: ', CAST(stock.`timestamp` AS STRING)
            ),
            'source:shared.digital.products.eligibility',
            CONCAT(
                'Partition: ', CAST(elig.`partition` AS STRING),
                ', Offset: ', CAST(elig.`offset` AS STRING),
                ', Timestamp: ', CAST(elig.`timestamp` AS STRING)
            ),
            'source:shared.digital.products.location',
            IF(
                loc.upcId IS NULL,
                'empty',
                CONCAT(
                    'Partition: ', CAST(loc.`partition` AS STRING),
                    ', Offset: ', CAST(loc.`offset` AS STRING),
                    ', Timestamp: ', CAST(loc.`timestamp` AS STRING)
                )
            ),
            'source:shared.digital.products.product-info',
            IF(
                info.upcId IS NULL,
                'empty',
                CONCAT(
                    'Partition: ', CAST(info.`partition` AS STRING),
                    ', Offset: ', CAST(info.`offset` AS STRING),
                    ', Timestamp: ', CAST(info.`timestamp` AS STRING)
                )
            )
        ] AS headers
    FROM `shared.digital.products.stock` AS stock
    INNER JOIN `shared.digital.products.eligibility` AS elig
        ON (
            stock.`upcId` = elig.`upcId` AND
            stock.`storeId` = elig.`storeId` AND
            stock.`upcTypeName` = elig.`upcTypeName`
        )
    LEFT JOIN `shared.digital.products.location` AS loc
        ON (
            stock.`upcId` = loc.`upcId` AND
            stock.`storeId` = loc.`storeId`
        )
    LEFT JOIN `shared.digital.products.product-info` AS info
        ON (
            stock.`upcId` = info.`upcId` AND
            stock.`upcTypeName` = info.`upcTypeName`
        )
);