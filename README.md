# Flinksql integration tests using TableAPI   
Introduction : 
This repo is created as a reference project for creating <b>integration</b> tests when using Confluent Flink SQL. Currently, this code uses the Flink's Table API to [submit](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L47) a flink SQL. Sample data was inserted manually, and that data is present in the [insert_data.sql](https://github.com/bjaggi/flinksql-int-test/blob/main/src/main/resources/insert_data.sql). You can also choose to insert data programmatically, this code has been developed from the [sample project](https://github.com/confluentinc/learn-apache-flink-table-api-for-java-exercises/blob/main/solutions/03-building-a-streaming-pipeline/src/test/java/marketplace/CustomerServiceIntegrationTest.java#L43).


This Integration framework is based on :
- A Java/Maven code( Current repo on the git)
  - [Unit Test code in Java](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L31)
  - [Resources folder](https://github.com/bjaggi/flinksql-int-test/tree/main/src/main/resources)
- A real Confluent Cloud cluster & Flink ( configure : `resources/cloud.properties` )


<h2>Strategy:</h2> 
The Integration Test can be run manually or automatically via github actions ( on any code change, github actions can run change run a    ``mvn package/ mvn  test`` ) . Which would test/assert the committed code and validate the [input vs output data](https://github.com/bjaggi/flinksql-int-test/blob/main/src/test/java/io/confluent/flink/examples/HybrisStoreProductServiceTest.java#L61)   

<br><br><br><br>
Note : CTAS( Create Table As) is currently not available in the current version of Table API `<confluent-plugin.version>1.20-50</confluent-plugin.version>`. CTAS in TableAPI, should be released in the next version Q2/2025. In order to accomadate this, we are running a ``SELECT`` instead of ``CREATE TABLE AS SELECT * FROM...``
