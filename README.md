# Flinksql integration tests using TableAPI     
<br>
<b> <font size="10">Introduction : </font ></b>

This repo is created as a reference project for creating <b>integration</b> tests when using Confluent Flink SQL. Currently, this code uses the Flink's Table API to [submit](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L47). A flink SQL. Sample data was inserted manually, and that data is present in the [insert_data.sql](https://github.com/bjaggi/flinksql-int-test/blob/main/src/main/resources/insert_data.sql).
<br>
You can also choose to insert data programmatically, this code has been developed from the [sample project](https://github.com/confluentinc/learn-apache-flink-table-api-for-java-exercises/blob/main/solutions/03-building-a-streaming-pipeline/src/test/java/marketplace/CustomerServiceIntegrationTest.java#L43).
Data could also be inserted programmatically for each test case. 

This Integration framework is based on :
- A Java/Maven code( Current repo on the git)
  - [Unit Test code in Java](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L31)
  - [Resources folder](https://github.com/bjaggi/flinksql-int-test/tree/main/src/main/resources)
- A real Confluent Cloud cluster & Flink ( configure : `resources/cloud.properties` )


<b> <font size="10">Strategy:</font ></b> 

- The Integration Test can be run manually or <b>automatically via github actions</b> ( ie: on any SQL code change & git commit, github actions can run change run a    `mvn package/ mvn test`.  )
  - Which would test/assert the committed code and validate the [input vs output data](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L61). 
- You can also add a step where only after all tests are passed SQL is pushed to the targetted environment. 
- It is generally recommended to have one unit case per test case/scenario, it may be required to insert data relevant to that test case and this is supported by the Table API.

<b> <font size="10">How to run the tests:</font ></b> 

- Create a file called `cloud.properties` in the  `resources` folder & fill the following details:    
```html
client.organization-id:
client.environment-id:
client.flink-api-key:
client.flink-api-secret : 
client.compute-pool-id:
client.cloud: 
client.region: 
```
- execute `mvn package` or manually run `HybrisStoreProductServiceTest.java` in the `test` folder.
- Make a note of the Job-Id in the logs, this is the Job that will be run on your CC Flink. example ` job id : [table-api-2025-03-25-104344-dd870732-f129-436c-b433-999f3319aaed-sql]
  ` 
- If tests fail, check the reason. Very likely its failing for timeout of expected data not matching with input data.   


<br><br>
Note : CTAS( Create Table As) is currently not available in the current version of Table API `<confluent-plugin.version>1.20-50</confluent-plugin.version>`. CTAS in TableAPI, should be released in the next version Q2/2025. In order to accomadate this, we are running a ``SELECT`` instead of ``CREATE TABLE AS SELECT * FROM...``
