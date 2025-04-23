package io.confluent.flink.examples;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import io.confluent.flink.plugin.ConfluentSettings;
import io.confluent.flink.examples.helper.SqlReader;
import io.confluent.flink.examples.helper.RowComparator;

import java.util.*;
import java.util.stream.Stream;

import static io.confluent.flink.examples.HybrisStoreProductService.hybrisStoreProductTableName;
import static org.junit.jupiter.api.Assertions.*;

@Tag("IntegrationTest")
public class HybrisStoreProductServiceTest extends FlinkIntegrationTest {

    private HybrisStoreProductService hybrisStoreProductService;

    @Override
    public void setup() {
        EnvironmentSettings settings = ConfluentSettings.fromResource("/cloud.properties");
        TableEnvironment env = TableEnvironment.create(settings);

        System.out.println("Starting Flink Integration Tests!");
        System.out.println();System.out.println();
        SqlReader.listResources(env);

        hybrisStoreProductService = new HybrisStoreProductService(
                env,
                hybrisStoreProductTableName, hybrisStoreProductTableName
        );
    }

    @Test
    @Timeout(240)
    public void hybrisQueryTest() throws Exception {
        // Generate some data.
        List<Row> data = Stream.generate(() -> new SampleData().build())
                .limit(5)
                .toList();

        // Execute the query.
        TableResult results = hybrisStoreProductService.executeHybrisStoreProductQuery();
        System.out.println(" job id : " + results.getJobClient().stream().toList());

        // Fetch the actual results.
        List<Row> actual = fetchRows(results)
                .limit(data.size())
                .toList();

        // Assert on the results.
       // assertEquals(data.get(0).getField(0), actual.get(0).getField(0));
               // Compare results using the new comparator
               for (int i = 0; i < data.size(); i++) {
                RowComparator.ComparisonResult comparison = RowComparator.compareRows(data.get(i), actual.get(i));
                assertTrue(comparison.isEqual(), "Row " + i + " comparison failed: " + comparison.getMessage());
            }
            

        // Set<String> expectedFields = new HashSet<>(Arrays.asList(
        //         "customer_id", "name", "address", "postcode", "city", "email"
        // ));
         //assertEquals(expectedFields, actual.getFirst().getFieldNames(true));
    }
}
