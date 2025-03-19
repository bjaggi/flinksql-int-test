package io.confluent.flink.examples;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.stream.Stream;

import static io.confluent.flink.examples.HybrisStoreProductService.hybrisStoreProductTableName;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
public class HybrisStoreProductServiceTest extends FlinkIntegrationTest {



    private HybrisStoreProductService hybrisStoreProductService;

    @Override
    public void setup() {
        hybrisStoreProductService = new HybrisStoreProductService(
                env,
                hybrisStoreProductTableName, hybrisStoreProductTableName
        );
    }

    @Test
    @Timeout(90)
    public void allCustomers_shouldReturnTheDetailsOfAllCustomers() throws Exception {
        // Clean up any tables left over from previously executing this test.
        //deleteTable(hybrisStoreProductTableName);

        // Create a temporary customers table.
       //  createTemporaryTable(hybrisStoreProductTableName, customersTableDefinition);

        // Generate some customers.
        List<Row> customers = Stream.generate(() -> new CustomerBuilder().build())
                .limit(5)
                .toList();

        // Push the customers into the temporary table.
        //env.fromValues(customers).insertInto(hybrisStoreProductTableName).execute();

        // Execute the query.
         TableResult results = hybrisStoreProductService.creatHybrisStoreProductTable();

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
                .limit(customers.size())
                .toList();

        // Assert on the results.
        assertEquals(new HashSet<>(customers), new HashSet<>(actual));

        Set<String> expectedFields = new HashSet<>(Arrays.asList(
                "customer_id", "name", "address", "postcode", "city", "email"
        ));
         assertEquals(expectedFields, actual.getFirst().getFieldNames(true));
    }
}