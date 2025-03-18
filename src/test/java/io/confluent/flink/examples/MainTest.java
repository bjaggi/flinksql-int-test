package io.confluent.flink.examples;


import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
class MainTest {

    private final String ordersTableName = "`flink-table-api-java`.`marketplace`.`orders-temp`";
    private final String orderQualifiedForFreeShippingTableName = "`flink-table-api-java`.`marketplace`.`order-qualified-for-free-shipping-temp`";
    private final String orderQualifiedForFreeShippingShortTableName = "order-qualified-for-free-shipping-temp";

    private OrderService orderService;
    @Override
    public void setup() {
        orderService = new OrderService(
                env,
                ordersTableName,
                orderQualifiedForFreeShippingTableName
        );
    }

    @Test
    @Timeout(90)
    public void ordersOver50Dollars_shouldOnlyReturnOrdersWithAPriceOf50DollarsOrMore() {
        // Clean up any tables left over from previously executing this test.
        deleteTable(ordersTableName);

        // Create a temporary orders table.
        createTemporaryTable(ordersTableName, ordersTableDefinition);

        // Create a set of orders with fixed prices
        Double[] prices = new Double[] { 25d, 49d, 50d, 51d, 75d };

        List<Row> orders = Arrays.stream(prices).map(price ->
                new OrderBuilder().withPrice(price).build()
        ).toList();

        // Push the orders into the temporary table.
        env.fromValues(orders).insertInto(ordersTableName).execute();

        // Execute the query.
        TableResult results = orderService.ordersOver50Dollars();

        // Build the expected results.
        List<Row> expected = orders.stream().filter(row -> row.<Double>getFieldAs(indexOf("price")) >= 50).toList();

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
                .limit(expected.size())
                .toList();

        // Assert on the results.
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "order_id", "customer_id", "product_id", "price"
        ));
        assertTrue(actual.getFirst().getFieldNames(true).containsAll(expectedFields));
    }



}