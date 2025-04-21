INSERT INTO `external.digital.products.store-product` (
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
    price,
    isMap,
    alternateUnitOfMeasure,
    productRules,
    headers
)
(
SELECT
    sel.upcId,
    sel.storeId,
    sel.upcTypeName,
    sel.productId,
    sel.stockStatus,
    sel.stockStatusId,
    sel.storeBOH,
    sel.ilcPrimary,
    sel.ilcs,
    sel.isNewIlc,
    sel.isEligible,
    sel.isInStoreOnly,
    ROW (
        currPrice.basePrice,
        currPrice.promotionPrice,
        currPrice.clearancePrice,
        currPrice.priceEndDate,
        currPrice.buyQuantity,
        currPrice.sellQuantity,
        currPrice.eachPrice,
        currPrice.priceChangeMethod,
        currPrice.percentageOff,
        currPrice.dollarOff,
        currPrice.basePricePerSoldByUnit,
        currPrice.customerPrice,
        currPrice.customerPricePerSoldByUnit,
        currPrice.discountValue,
        currPrice.discountValuePerSoldByUnit,
        currPrice.pricingUnit,
        currPrice.soldByUnit,
        currPrice.unitOfMeasureQuantity,
        currPrice.avgSoldByUnitsPerPricingUnit,
        currPrice.avgPricingUnitsPerSoldByUnit,
        currPrice.activePriceType,
        currPrice.priceText,
        currPrice.discountText
    ) AS price,
    currPrice.isMap,
    currPrice.alternateUnitOfMeasure,
    ROW (
        res.ageRestriction,
        res.alcoholRestriction,
        res.taxDeposit
    ) AS productRules,
    MAP [
        'source:shared.digital.products.stock-eligibility-location',
        CONCAT(
            'Partition: ', CAST(sel.`partition` AS STRING),
            ', Offset: ', CAST(sel.`offset` AS STRING),
            ', Timestamp: ', CAST(sel.`timestamp` AS STRING)
        ),
        'source:shared.digital.products.price-current',
        CONCAT(
            'Partition: ', CAST(currPrice.`partition` AS STRING),
            ', Offset: ', CAST(currPrice.`offset` AS STRING),
            ', Timestamp: ', CAST(currPrice.`timestamp` AS STRING)
        ),
        'source:shared.digital.products.product-unit-restriction',
        IF (
            res.upcId IS NULL,
            'empty',
            CONCAT(
                'Partition: ', CAST(res.`partition` AS STRING),
                ', Offset: ', CAST(res.`offset` AS STRING),
                ', Timestamp: ', CAST(res.`timestamp` AS STRING)
            )
        )
    ] AS headers
FROM `shared.digital.products.stock-eligibility-location` AS sel
INNER JOIN `shared.digital.products.price-current` AS currPrice
    ON (
        sel.`upcId` = currPrice.`upcId` AND
        sel.`storeId` = currPrice.`storeId` AND
        sel.`upcTypeName` = currPrice.`upcTypeName`
    )
LEFT JOIN `shared.digital.products.product-unit-restriction` AS res
    ON (
        sel.`upcId` = res.`upcId` AND
        sel.`storeId` = res.`storeId` AND
        sel.`upcTypeName` = res.`upcTypeName`
    )
);import java.io.BufferedReader;
  import java.io.FileReader;
  import java.io.IOException;

  public class CSVReader {
      public static void main(String[] args) {
          String csvFile = "path/to/your/file.csv";
          String line;
          String csvSplitBy = ",";

          try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
              while ((line = br.readLine()) != null) {
                  String[] data = line.split(csvSplitBy);
                  System.out.println(Arrays.toString(data));
              }
          } catch (IOException e) {
              e.printStackTrace();
          }
      }
  }
