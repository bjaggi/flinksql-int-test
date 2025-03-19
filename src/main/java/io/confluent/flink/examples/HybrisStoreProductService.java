package io.confluent.flink.examples;

import org.apache.flink.table.api.*;

public class HybrisStoreProductService {
    private final TableEnvironment env;
    private final String ordersTableName;
    private final String freeShippingTableName;
    public static final String hybrisStoreProductTableName = "`Development`.`Digital-Public-Development`.`hybris-store-product-stock`";

    public HybrisStoreProductService(
        TableEnvironment env,
        String ordersTableName,
        String freeShippingTableName
    ) {
        this.env = env;
        this.ordersTableName = ordersTableName;
        this.freeShippingTableName = freeShippingTableName;
    }

    public TableResult creatHybrisStoreProductTable() {
        return env.executeSql(
            "INSERT INTO "+ hybrisStoreProductTableName+
                  " SELECT \n" +
                    "    si.upcId,\n" +
                    "    si.storeId, \n" +
                    "    si.productId,\n" +
                    "    si.upcTypeName,\n" +
                    "    CASE si.isSellableFlag\n" +
                    "      WHEN 1 THEN 'inStock'\n" +
                    "      WHEN 2 THEN 'lowStock'\n" +
                    "      ELSE 'outOfStock'\n" +
                    "    END AS stockStatus,\n" +
                    "    si.isSellableFlag AS stockStatusId, \n" +
                    "    si.storeBalanceOnHandDecimalQuantity AS storeBOH, \n" +
                    "    ilc.ilcPrimary AS ilcPrimary, \n" +
                    "    ilc.ilcs AS ilcs, \n" +
                    "    true AS isNewIlc, \n" +
                    "    elig.productEligibility AS isEligible,\n" +
                    "    CASE\n" +
                    "      WHEN ph.isInStoreOnly IS NULL\n" +
                    "      THEN false\n" +
                    "      ELSE ph.isInStoreOnly\n" +
                    "    END as isInStoreOnly,\n" +
                    "    MAP[\n" +
                    "        'source:shared.digital.products.store-item', \n" +
                    "        CONCAT(\n" +
                    "          'Partition: ', CAST(si.`partition` AS STRING), \n" +
                    "          ', Offset: ', CAST(si.`offset` AS STRING), \n" +
                    "          ' Timestamp: ', CAST( EXTRACT(EPOCH FROM si.`timestamp`)  AS STRING ) \n" +
                    "        ),\n" +
                    "        'source:shared.digital.products.product-eligibility', \n" +
                    "        CONCAT(\n" +
                    "          'Partition: ', CAST(elig.`partition` AS STRING), \n" +
                    "          ', Offset: ', CAST(elig.`offset` AS STRING), \n" +
                    "          ' Timestamp: ', CAST( EXTRACT(EPOCH FROM si.`timestamp`)  AS STRING ) \n" +
                    "          ),\n" +
                    "        'source:shared.digital.products.ilc', IF (ilc.upcId IS NULL, 'empty', \n" +
                    "        CONCAT(\n" +
                    "            'Partition: ', CAST(ilc.`partition` AS STRING), \n" +
                    "            ', Offset: ', CAST(ilc.`offset` AS STRING), \n" +
                    "           ' Timestamp: ', CAST( EXTRACT(EPOCH FROM si.`timestamp`)  AS STRING ) \n" +
                    "        )),\n" +
                    "        'source:shared.digital.products.product-hierarchy', IF (ph.upcId IS NULL, 'empty', \n" +
                    "        CONCAT(\n" +
                    "            'Partition: ', CAST(ph.`partition` AS STRING), \n" +
                    "            ', Offset: ', CAST(ph.`offset` AS STRING)\n" +
                   // "            --' Timestamp: ',  CAST(ph.`timestamp`  AS STRING) \n" +
                    "        ))\n" +
                    "    ] AS headers\n" +
                    "FROM\n" +
                    "  `Development`.`Digital-Public-Development`.`shared.digital.products.store-item` si\n" +
                    "  INNER JOIN `Development`.`Digital-Public-Development`.`shared.digital.products.product-eligibility` elig \n" +
                    "    ON (\n" +
                    "      si.storeId = elig.storeId AND \n" +
                    "      si.upcId = elig.upcId\n" +
                    "    ) \n" +
                    "  LEFT JOIN `Development`.`Digital-Public-Development`.`shared.digital.products.ilc` ilc\n" +
                    "    ON (\n" +
                    "      si.storeId = ilc.storeId AND \n" +
                    "      si.upcId = ilc.upcId ) \n" +
                    "  LEFT JOIN `Development`.`Digital-Public-Development`.`shared.digital.products.product-hierarchy` ph\n" +
                    "      ON si.upcId = ph.upcId\n" +
                    ";"
                    );
    }

//    public TableResult ordersOver50Dollars() {
//        return env.from(ordersTableName)
//            .select($("*"))
//            .where($("price").isGreaterOrEqual(50))
//            .execute();
//    }

//    public TableResult streamOrdersOver50Dollars() {
//        return env.from(ordersTableName)
//            .where($("price").isGreaterOrEqual(50))
//            .select(
//                $("order_id"),
//                row(
//                    $("customer_id"),
//                    $("product_id"),
//                    $("price")
//                ).as("details")
//            )
//            .insertInto(freeShippingTableName)
//            .execute();
//    }
//
//    public TableResult pricesWithTax(BigDecimal taxAmount) {
//        return env.from(ordersTableName)
//            .select(
//                $("order_id"),
//                $("price")
//                    .cast(DataTypes.DECIMAL(10, 2))
//                    .as("original_price"),
//                $("price")
//                    .cast(DataTypes.DECIMAL(10, 2))
//                    .times(taxAmount)
//                    .round(2)
//                    .as("price_with_tax")
//            ).execute();
//    }
}