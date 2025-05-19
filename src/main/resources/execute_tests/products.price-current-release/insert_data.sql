EXECUTE STATEMENT SET BEGIN


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.price-current-stage` 
  (key_priceId, key_window, currentPriceId, currentPricePk, operations, priceId, `window`)
VALUES
  ('3027900067:UPCA:REGULAR:2025050118', '2025050118', '3027900067:UPCA:REGULAR:2025050118', '3027900067:UPCA:72:CURRENTPRICE', 
   '[{"operation":"/regular","priceChange":{"merchPriceId":"3027900067:UPCA:REGULAR:2025050118","merchPricePartitionKey":"3027900067:UPCA:REGULAR:2025050118:MERCHPRICE","amount":9.99,"expiresOn":"2025-05-01T19:00:00","priceChangeMethod":"$ OFF REG","buyQuantity":1,"sellQuantity":0,"eachPrice":9.99,"limitBuyQuantity":0,"percentageOff":0.0,"dollarOff":0.0}}]', 
   '3027900067:UPCA:REGULAR:2025050118', 
   '2025050118');
--VALUES ('19685318515', 20, 'UPCA', '5224494', true);


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` 
(`key`, `preprocessWindow`, `window`)
VALUES 
( '2025050118', '2025050118', '2025050118' );


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` 
(`key`, `preprocessWindow`, `window`)
VALUES 
( '2025050118', '2025050118', '2025050118' );


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` 
(`key`, `preprocessWindow`, `window`)
VALUES 
( '2025050118', '2025050118', '2025050118' );


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` 
(`key`, `preprocessWindow`, `window`)
VALUES 
( '2025050118', '2025050118', '2025050118' );


INSERT INTO `Development`.`Digital-Public-Development`.`private.digital.products.process-scheduled-price-window` 
(`key`, `preprocessWindow`, `window`)
VALUES 
( '2025050118', '2025050118', '2025050118' );



END;
