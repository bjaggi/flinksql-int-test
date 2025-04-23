EXECUTE STATEMENT SET BEGIN
//DROP TABLE `Development`.`Digital-Public-Development`.`shared.digital.products.product-eligibility`;

INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.product-eligibility`
(upcId, storeId, upcTypeName, productId, productEligibility)
VALUES ('19685318515', 20, 'UPCA', '5224494', true);


INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.product-hierarchy` (
    `upcId`, `productId`, `itemName`, `itemDescription`, `soldByUnit`, `upcTypeName`, `imageFront`, `isProductAgeRestricted`, `isAlcohol`, `isChokingHazard`, `isFoodStampEligible`,
    `brandDescription`, `brandName`, `category`, `descriptiveFeatures`, `disclaimer`, `imageBack`, `imageBottom`, `imageLeft`, `imageRight`, `imageTiltLeft`,
    `imageTiltRight`, `imageTop`, `isBopas`, `isHomeDeliveryAvailable`, `isPrimaryUpc`, `isPrivateBrand`, `isReceiveByWeightEligible`, `marginPercent`,
    `productDetails`, `sellingSize`, `taxonomyId`, `taxonomyParentId`, `unitOfMeasureQuantity`, `isCurbsideEligible`, `soldByUnitDescription`, `priceUnit`, `isPriceByWeight`,
    `productStatus`, `eventType`, `isInStoreOnly`, `ecommEffectiveDate`, `averagePoundsPerEach`, `countryOfOrigin`, `isMap`, `alternateUnitOfMeasure`, `upcCheckDigit`, `barCodeId`,
    `advertisingPriceMinimumAmount`
) VALUES (
    '19685318515', '5224494', 'Sample Product', 'Sample product description.', 'package', 'UPCA', 'http://example.com/imagefront.jpg', TRUE, FALSE, FALSE, TRUE,
    'Example brand description.', 'Example Brand', 'Example Category', 'Feature1, Feature2', 'This is a disclaimer.', 'http://example.com/imageback.jpg',
    'http://example.com/imagebottom.jpg', 'http://example.com/imageleft.jpg', 'http://example.com/imageright.jpg', 'http://example.com/imagelt.jpg',
    'http://example.com/imagetr.jpg', 'http://example.com/imagetop.jpg', TRUE, TRUE, TRUE, FALSE, TRUE, 20.5, 'Detailed product information goes here.',
    '500ml', 'TAX123', 'TXP456', 1.0, TRUE, 'Describes unit sold by', 'unit', TRUE, 'Available', 'Event1', FALSE, '2025-01-01', 2.5, 'Country1', TRUE, 'AlternateUnit',
    5, 'BAR1234567890', 10.99
);




INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.ilc`
(`upcId`, `storeId`, `productId`, `ilcPrimary`, `ilcs`  )
VALUES
('19685318515', 20, '5224494', 'ILC12345', 'ILC12345, ILC67890');



INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.ilc`
(`upcId`, `storeId`, `productId`, `ilcPrimary`, `ilcs`)
VALUES
('19685318515', 20, '5224494', 'ILC12345', 'ILC12345, ILC67890');




INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.store-item`
( `upcId`,`storeId`,`upcTypeName`,`productId`,`isSellableFlag`,`storeBalanceOnHandDecimalQuantity`,`headers` )
VALUES
( '19685318515',20,'UPCA','5224494', 1,150.75, MAP['headerKey1', 'headerValue1', 'headerKey2', 'headerValue2']);





INSERT INTO `Development`.`Digital-Public-Development`.`shared.digital.products.product-eligibility`
(`upcId`, `storeId`, `upcTypeName`, `productId`, `productEligibility`)
VALUES
( '19685318515', 20, 'UPCA',  '5224494', TRUE );


END;
