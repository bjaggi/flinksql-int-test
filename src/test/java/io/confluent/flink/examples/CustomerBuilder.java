package io.confluent.flink.examples;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;

import java.util.Random;

class CustomerBuilder {
    private int customerId;
    private String name;
    private String address;
    private String postCode;
    private String city;
    private String email;
    private String upcId;
    private String storeId;
    private String productId;
    private String upcTypeName;
    private String stockStatus;
    private String stockStatusId;
    private String storeBOH;
    private String ilcPrimary;
    private String ilcs;
    private boolean isNewIlc;
    private boolean isEligible;
    private boolean isInStoreOnly;
    //private String headers;






    private final Random rnd = new Random(System.currentTimeMillis());



    //[+I[19685318515, 20, 5224494, UPCA, inStock, 1, 150.75, null, null, true, true, false, {source:shared.digital.products.store-item=Partition: 1, Offset: 4, source:shared.digital.products.product-hierarchy=empty, source:shared.digital.products.ilc=empt ...
    public CustomerBuilder() {
//        customerId = rnd.nextInt(1000);
//        name = "Name" + rnd.nextInt(1000);
//        address = "Address" + rnd.nextInt(1000);
//        postCode = "PostCode" + rnd.nextInt(1000);
//        city = "City" + rnd.nextInt(1000);
//        email = "Email" + rnd.nextInt(1000);


        this.upcId = "19685318515";
        this.storeId = "20";
        this.productId = "5224494";
        this.upcTypeName = "UPCA";
        this.stockStatus = "inStock";
        this.stockStatusId = "1";
        this.storeBOH = "150.75";
        this.ilcPrimary = "ILC12345";
        this.ilcs = "ILC12345";
        this.isNewIlc = true;
        this.isEligible = true;
        this.isInStoreOnly = false;
       // this.headers = "{source:shared.digital.products.store-item=Partition: 1, Offset: 4, source:shared.digital.products.product-hierarchy=empty, source:shared.digital.products.ilc=empty, source:shared.digital.products.product-eligibility=Partition: 2, Offset: 0}";

    }

    public CustomerBuilder withCustomerId(int customerId) {
        this.customerId = customerId;
        return this;
    }

    public CustomerBuilder withName(String name) {
        this.name = name;
        return this;
    }

    public CustomerBuilder withAddress(String address) {
        this.address = address;
        return this;
    }

    public CustomerBuilder withPostCode(String postCode) {
        this.postCode = postCode;
        return this;
    }

    public CustomerBuilder withCity(String city) {
        this.city = city;
        return this;
    }

    public CustomerBuilder withEmail(String email) {
        this.email = email;
        return this;
    }

    public Row build() {
        //return Row.of(customerId, name, address, postCode, city, email);
        return Row.of(upcId, storeId, productId, upcTypeName, stockStatus, stockStatusId,storeBOH, ilcPrimary, ilcs, isNewIlc, isEligible, isInStoreOnly);
    }
}