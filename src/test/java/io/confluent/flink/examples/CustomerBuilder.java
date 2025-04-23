package io.confluent.flink.examples;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;

import java.util.Random;

class SampleData {
    private int customerId;
    private String name;
    private String address;
    private String postCode;
    private String city;
    private String email;
    private String upcId;
    private long storeId;
    private String productId;
    private String upcTypeName;
    private String stockStatus;
    private long stockStatusId;
    private double storeBOH;
    private String ilcPrimary;
    private String ilcs;
    private boolean isNewIlc;
    private boolean isEligible;
    private boolean isInStoreOnly;
    //private String headers;

    private final Random rnd = new Random(System.currentTimeMillis());

    public SampleData() {
        this.upcId = "19685318515";
        this.storeId = 20;
        this.productId = "5224494";
        this.upcTypeName = "UPCA";
        this.stockStatus = "inStock";
        this.stockStatusId = 1;
        this.storeBOH = 150.75;
        this.ilcPrimary = null;
        this.ilcs = null;
        this.isNewIlc = true;
        this.isEligible = true;
        this.isInStoreOnly = false;
        //this.headers = "{source:shared.digital.products.store-item=Partition: 1, Offset: 4, source:shared.digital.products.product-hierarchy=empty, source:shared.digital.products.ilc=empty, source:shared.digital.products.product-eligibility=Partition: 2, Offset: 0}";
    }

    public SampleData withCustomerId(int customerId) {
        this.customerId = customerId;
        return this;
    }

    public SampleData withName(String name) {
        this.name = name;
        return this;
    }

    public SampleData withAddress(String address) {
        this.address = address;
        return this;
    }

    public SampleData withPostCode(String postCode) {
        this.postCode = postCode;
        return this;
    }

    public SampleData withCity(String city) {
        this.city = city;
        return this;
    }

    public SampleData withEmail(String email) {
        this.email = email;
        return this;
    }

    public Row build() {
        //return Row.of(upcId, storeId, productId, upcTypeName, stockStatus, stockStatusId, storeBOH, ilcPrimary, ilcs, isNewIlc, isEligible, isInStoreOnly, headers);
        return Row.of(upcId, storeId, productId, upcTypeName, stockStatus, stockStatusId, storeBOH, ilcPrimary, ilcs, isNewIlc, isEligible, isInStoreOnly);
    }
}