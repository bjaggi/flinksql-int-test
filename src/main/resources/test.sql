INSERT INTO `shared.digital.products.hybris.store-product-stock` (
  upcId,
  storeId,
  productId,
  upcTypeName,
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
    si.upcId,
    si.storeId,
    si.productId,
    si.upcTypeName,
    CASE si.isSellableFlag
      WHEN 1 THEN 'inStock'
      WHEN 2 THEN 'lowStock'
      ELSE 'outOfStock'
    END AS stockStatus,
    si.isSellableFlag AS stockStatusId,
    si.storeBalanceOnHandDecimalQuantity AS storeBOH,
    ilc.ilcPrimary AS ilcPrimary,
    ilc.ilcs AS ilcs,
    true AS isNewIlc,
    elig.productEligibility AS isEligible,
    CASE
      WHEN ph.isInStoreOnly IS NULL
      THEN false
      ELSE ph.isInStoreOnly
    END as isInStoreOnly,
    MAP[
        'source:shared.digital.products.store-item',
        CONCAT(
          'Partition: ', CAST(si.`partition` AS STRING),
          ', Offset: ', CAST(si.`offset` AS STRING),
          ', Timestamp: ', CAST(si.`timestamp` AS STRING)
        ),
        'source:shared.digital.products.product-eligibility',
        CONCAT(
          'Partition: ', CAST(elig.`partition` AS STRING),
          ', Offset: ', CAST(elig.`offset` AS STRING),
          ', Timestamp: ', CAST(elig.`timestamp` AS STRING)
          ),
        'source:shared.digital.products.ilc', IF (ilc.upcId IS NULL, 'empty',
        CONCAT(
            'Partition: ', CAST(ilc.`partition` AS STRING),
            ', Offset: ', CAST(ilc.`offset` AS STRING),
            ', Timestamp: ', CAST(ilc.`timestamp` AS STRING)
        )),
        'source:shared.digital.products.product-hierarchy', IF (ph.upcId IS NULL, 'empty',
        CONCAT(
            'Partition: ', CAST(ph.`partition` AS STRING),
            ', Offset: ', CAST(ph.`offset` AS STRING),
            ', Timestamp: ', CAST(ph.`timestamp` AS STRING)
        ))
    ] AS headers
FROM
  `shared.digital.products.store-item` si
  INNER JOIN `shared.digital.products.product-eligibility` elig
    ON (
      si.storeId = elig.storeId AND
      si.upcId = elig.upcId
    )
  LEFT JOIN `shared.digital.products.ilc` ilc
    ON (
      si.storeId = ilc.storeId AND
      si.upcId = ilc.upcId )
  LEFT JOIN `shared.digital.products.product-hierarchy` ph
      ON si.upcId = ph.upcId
)