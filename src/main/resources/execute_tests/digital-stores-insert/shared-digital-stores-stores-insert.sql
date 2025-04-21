INSERT INTO `shared.digital.stores.stores`
(storeId,
hasAlcoholPickup,
hasAlcoholDelivery,
holidays,
isOpen24Hours,
mfcUnitId,
pickupType,
gasStationAmenities,
fuelInformation,
hours,
isMobilePaymentEnabled,
storeAddress,
storeCity,
storeState,
storeZip,
storeType,
storePhoneNumber,
storeName,
storeDirectorName,
pharmacyAddress,
pharmacyPhoneNumber,
storeLatitude,
storeLongitude,
timezone,
hasDaylightSavings,
deliPhoneNumber,
country,
serviceTypes,
tenants,
curbsideEligibility,
curbsidePartner,
payment
)
(
SELECT
storeId,
hasAlcoholPickup,
hasAlcoholDelivery,
holidays,
isOpen24Hours,
mfcUnitId,
pickupType,
gasStationAmenities,
fuelInformation,
hours,
isMobilePaymentEnabled,
storeAddress,
storeCity,
storeState,
storeZip,
storeType,
storePhoneNumber,
storeName,
storeDirectorName,
pharmacyAddress,
pharmacyPhoneNumber,
storeLatitude,
storeLongitude,
timezone,
hasDaylightSavings,
deliPhoneNumber,
country,
serviceTypes,
tenants,
curbsideEligibility,
curbsidePartner,
payment
 FROM
(SELECT
	 ROW_NUMBER() OVER (PARTITION BY UnitId,IsAlcoholPickupable,IsAlcoholDeliverable,HolidayInfo,FulfillmentInfo,
   IsMobilePaymentEnabled,
   Name,Address,City,State,Zip,UnitType,
   PhoneNumber,Name,StoreDirName,PharmAddress,
   PharmPhone,Latitude,Longitude,TimeZone,
   DaylightSavings,DeliOrderPhone,Tenants,CurbsideAllow
  ORDER BY $rowtime ASC) AS row_num,
  CAST(UnitId as INT) AS storeId,
  IsAlcoholPickupable AS hasAlcoholPickup,
  IsAlcoholDeliverable AS hasAlcoholDelivery,
  CASE WHEN HolidayInfo IS NOT NULL AND CARDINALITY(HolidayInfo) > 0
  THEN
  ARRAY[ROW(HolidayInfo[1].Id,CAST(if(HolidayInfo[1].ClosedAllDay='Y',true,false) AS boolean))]
  ELSE NULL END AS holidays,
  CAST(if(Store24HrsFlag='Y',true,false) AS boolean) AS isOpen24Hours,
  MfcUnitId AS mfcUnitId,
  CASE WHEN FulfillmentInfo IS NOT NULL AND CARDINALITY(FulfillmentInfo) > 0
  THEN
  FulfillmentInfo[1].FulfillmentTypeName
  ELSE NULL END AS pickupType,
  JSON_QUERY(JSON_STRING(GasStationAmenities),'$[*].AmentityType' RETURNING ARRAY<STRING>) AS gasStationAmenities,
  FuelPrices AS fuelInformation,
  ARRAY_REMOVE(ARRAY[
  CASE WHEN GasStationHours IS NOT NULL AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[1].OpenTime IS NOT NULL AND CAST(GasStationHours[1].OpenTime AS STRING) <> '00:00:00'
   AND GasStationHours[1].CloseTime IS NOT NULL AND GasStationHours[1].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[1].DayOfTheWeek,CAST(GasStationHours[1].OpenTime AS STRING),
  CAST(GasStationHours[1].CloseTime AS STRING))
  ELSE NULL END,
  CASE WHEN GasStationHours IS NOT NULL  AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[2].OpenTime IS NOT NULL AND GasStationHours[2].OpenTime <> '00:00:00'
   AND GasStationHours[2].CloseTime IS NOT NULL AND GasStationHours[2].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[2].DayOfTheWeek,CAST(GasStationHours[2].OpenTime AS STRING),
  CAST(GasStationHours[2].CloseTime AS STRING))
   ELSE NULL END,
  CASE WHEN GasStationHours IS NOT NULL AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[3].OpenTime IS NOT NULL AND GasStationHours[3].OpenTime <> '00:00:00'
   AND GasStationHours[3].CloseTime IS NOT NULL AND GasStationHours[3].CloseTime > '00:00:00'
  THEN
 ROW('GasStation',GasStationHours[3].DayOfTheWeek,CAST(GasStationHours[3].OpenTime AS STRING),
  CAST(GasStationHours[3].CloseTime AS STRING))
  ELSE NULL END,
   CASE WHEN GasStationHours IS NOT NULL  AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[4].OpenTime IS NOT NULL AND GasStationHours[4].OpenTime <> '00:00:00'
   AND GasStationHours[4].CloseTime IS NOT NULL AND GasStationHours[4].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[4].DayOfTheWeek,CAST(GasStationHours[4].OpenTime AS STRING),
  CAST(GasStationHours[4].CloseTime AS STRING))
  ELSE NULL END,
  CASE WHEN GasStationHours IS NOT NULL  AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[5].OpenTime IS NOT NULL  AND GasStationHours[5].OpenTime <> '00:00:00'
   AND GasStationHours[5].CloseTime IS NOT NULL  AND GasStationHours[5].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[5].DayOfTheWeek,CAST(GasStationHours[5].OpenTime AS STRING),
  CAST(GasStationHours[5].CloseTime AS STRING))
  ELSE NULL END,
  CASE WHEN GasStationHours IS NOT NULL  AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[6].OpenTime IS NOT NULL  AND GasStationHours[6].OpenTime <> '00:00:00'
   AND GasStationHours[6].CloseTime IS NOT NULL  AND GasStationHours[6].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[6].DayOfTheWeek,CAST(GasStationHours[6].OpenTime AS STRING),
  CAST(GasStationHours[6].CloseTime AS STRING))
 ELSE NULL END,
  CASE WHEN GasStationHours IS NOT NULL  AND CARDINALITY(GasStationHours) > 0
   AND GasStationHours[7].OpenTime IS NOT NULL  AND GasStationHours[7].OpenTime <> '00:00:00'
   AND GasStationHours[7].CloseTime IS NOT NULL  AND GasStationHours[7].CloseTime > '00:00:00'
  THEN
  ROW('GasStation',GasStationHours[7].DayOfTheWeek,CAST(GasStationHours[7].OpenTime AS STRING),
  CAST(GasStationHours[7].CloseTime AS STRING))
  ELSE NULL END,
  CASE  WHEN PharmDailyOpen IS NOT NULL  AND PharmDailyClose IS NOT NULL
		THEN
		ROW('pharmacy','DAILY',DATE_FORMAT(PharmDailyOpen,'HH:mm:ss'),DATE_FORMAT(PharmDailyClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmSatOpen IS NOT NULL  AND PharmSatClose IS NOT NULL
		THEN
		ROW('pharmacy','SATURDAY',DATE_FORMAT(PharmSatOpen,'HH:mm:ss'),DATE_FORMAT(PharmSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmSunOpen IS NOT NULL AND PharmSunClose IS NOT NULL
		THEN
		ROW('pharmacy','SUNDAY',DATE_FORMAT(PharmSunOpen,'HH:mm:ss'),DATE_FORMAT(PharmSunClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmHolidayOpen IS NOT NULL AND PharmHolidayClose IS NOT NULL
		THEN
		ROW('pharmacy','HOLIDAY',DATE_FORMAT(PharmHolidayOpen,'HH:mm:ss'),DATE_FORMAT(PharmHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN CurbsideWeekdayOpen IS NOT NULL AND CurbsideWeekdayClose IS NOT NULL
		THEN
		ROW('curbside','Weekday',DATE_FORMAT(CurbsideWeekdayOpen,'HH:mm:ss'),DATE_FORMAT(CurbsideWeekdayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN CurbsideSatOpen IS NOT NULL AND CurbsideSatClose IS NOT NULL
		THEN
		ROW('curbside','SATURDAY',DATE_FORMAT(CurbsideSatOpen,'HH:mm:ss'),DATE_FORMAT(CurbsideSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN CurbsideHolidayOpen IS NOT NULL AND CurbsideHolidayClose IS NOT NULL
		THEN
		ROW('curbside','HOLIDAY',DATE_FORMAT(CurbsideHolidayOpen,'HH:mm:ss'),DATE_FORMAT(CurbsideHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PreOrderWeekdayOpen IS NOT NULL AND PreOrderWeekdayClose IS NOT NULL
		THEN
		ROW('storePreOrder','WEEKDAY',DATE_FORMAT(PreOrderWeekdayOpen,'HH:mm:ss'),DATE_FORMAT(PreOrderWeekdayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PreOrderSatOpen IS NOT NULL AND PreOrderSatClose IS NOT NULL
		THEN
		ROW('storePreOrder','SATURDAY',DATE_FORMAT(PreOrderSatOpen,'HH:mm:ss'),DATE_FORMAT(PreOrderSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PreOrderSunOpen IS NOT NULL AND PreOrderSunClose IS NOT NULL
		THEN
		ROW('storePreOrder','SUNDAY',DATE_FORMAT(PreOrderSunOpen,'HH:mm:ss'),DATE_FORMAT(PreOrderSunClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PreOrderHolidayOpen IS NOT NULL AND PreOrderHolidayClose IS NOT NULL
		THEN
		ROW('storePreOrder','HOLIDAY',DATE_FORMAT(PreOrderHolidayOpen,'HH:mm:ss'),DATE_FORMAT(PreOrderHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DeliOrderWeekdayOpen IS NOT NULL AND DeliOrderWeekdayClose IS NOT NULL
		THEN
		ROW('deliveryOrderSchedule','WEEKDAY',DATE_FORMAT(DeliOrderWeekdayOpen,'HH:mm:ss'),DATE_FORMAT(DeliOrderWeekdayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DeliOrderSatOpen IS NOT NULL AND DeliOrderSatClose IS NOT NULL
		THEN
		ROW('deliveryOrderSchedule','SATURDAY',DATE_FORMAT(DeliOrderSatOpen,'HH:mm:ss'),DATE_FORMAT(DeliOrderSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DeliOrderSunOpen IS NOT NULL AND DeliOrderSunClose IS NOT NULL
		THEN
		ROW('deliveryOrderSchedule','SUNDAY',DATE_FORMAT(DeliOrderSunOpen,'HH:mm:ss'),DATE_FORMAT(DeliOrderSunClose,'HH:mm:ss'))
		ELSE NULl END,
		CASE  WHEN DeliOrderHolidayOpen IS NOT NULL AND DeliOrderHolidayClose IS NOT NULL
		THEN
		ROW('deliveryOrderSchedule','HOLIDAY',DATE_FORMAT(DeliOrderHolidayOpen,'HH:mm:ss'),DATE_FORMAT(DeliOrderHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DlvryOrderWeekdayOpen IS NOT NULL AND DlvryOrderWeekdayClose IS NOT NULL
		THEN
		ROW('deliveryOrder','WEEKDAY',DATE_FORMAT(DlvryOrderWeekdayOpen,'HH:mm:ss'),DATE_FORMAT(DlvryOrderWeekdayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DlvryOrderSatOpen IS NOT NULL AND DlvryOrderSatClose IS NOT NULL
		THEN
		ROW('deliveryOrder','SATURDAY',DATE_FORMAT(DlvryOrderSatOpen,'HH:mm:ss'),DATE_FORMAT(DlvryOrderSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DlvryOrderSunOpen IS NOT NULL AND DlvryOrderSunClose IS NOT NULL
		THEN
		ROW('deliveryOrder','SUNDAY',DATE_FORMAT(DlvryOrderSunOpen,'HH:mm:ss'),DATE_FORMAT(DlvryOrderSunClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN DlvryOrderHolidayOpen IS NOT NULL AND DlvryOrderHolidayClose IS NOT NULL
		THEN
		ROW('deliveryOrder','HOLIDAY',DATE_FORMAT(DlvryOrderHolidayOpen,'HH:mm:ss'),DATE_FORMAT(DlvryOrderHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmDriveThroughDailyOpen IS NOT NULL AND PharmDriveThroughDailyClose IS NOT NULL
		THEN
		ROW('PharmDriveThrough','DAILY',DATE_FORMAT(PharmDriveThroughDailyOpen,'HH:mm:ss'),DATE_FORMAT(PharmDriveThroughDailyClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmDriveThroughSatOpen IS NOT NULL AND PharmDriveThroughSatClose IS NOT NULL
		THEN
		ROW('PharmDriveThrough','SATURDAY',DATE_FORMAT(PharmDriveThroughSatOpen,'HH:mm:ss'),DATE_FORMAT(PharmDriveThroughSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmDriveThroughSunOpen IS NOT NULL AND PharmDriveThroughSunClose IS NOT NULL
		THEN
		ROW('PharmDriveThrough','SUNDAY',DATE_FORMAT(PharmDriveThroughSunOpen,'HH:mm:ss'),DATE_FORMAT(PharmDriveThroughSunClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN PharmDriveThroughHolidayOpen IS NOT NULL AND PharmDriveThroughHolidayClose IS NOT NULL
		THEN
		ROW('PharmDriveThrough','HOLIDAY',DATE_FORMAT(PharmDriveThroughHolidayOpen,'HH:mm:ss'),DATE_FORMAT(PharmDriveThroughHolidayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN StoreWeekdayOpen IS NOT NULL AND StoreWeekdayClose IS NOT NULL
		THEN
		ROW('StoreSchedule','DAILY',DATE_FORMAT(StoreWeekdayOpen,'HH:mm:ss'),DATE_FORMAT(StoreWeekdayClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN StoreSatOpen IS NOT NULL AND StoreSatClose IS NOT NULL
		THEN
		ROW('storeSchedule','SATURDAY',DATE_FORMAT(StoreSatOpen,'HH:mm:ss'),DATE_FORMAT(StoreSatClose,'HH:mm:ss'))
		ELSE NULL END,
		CASE  WHEN StoreSunOpen IS NOT NULL AND StoreSunClose IS NOT NULL
		THEN
		ROW('storeSchedule','SUNDAY',DATE_FORMAT(StoreSunOpen,'HH:mm:ss'),DATE_FORMAT(StoreSunClose,'HH:mm:ss'))
		ELSE NULL END
		],NULL) AS hours,
		CAST(IF(IsMobilePaymentEnabled='Y',TRUE,FALSE) AS boolean) AS isMobilePaymentEnabled,
  Address AS storeAddress,
  City AS storeCity,
  State AS storeState,
  Zip AS storeZip,
  UnitType AS storeType,
  PhoneNumber AS storePhoneNumber,
  Name AS storeName,
  StoreDirName AS storeDirectorName,
  PharmAddress AS pharmacyAddress,
  PharmPhone AS pharmacyPhoneNumber,
  Latitude AS storeLatitude,
  Longitude AS storeLongitude,
  CASE TimeZone  WHEN 'E' THEN 'America/New_York'
                               WHEN 'C' THEN 'America/Chicago'
                               WHEN 'M' THEN 'America/Denver'
                               WHEN 'P' THEN 'America/Los_Angeles'
                              ELSE 'America/New_York'
                END AS timezone,
	CAST(if(DaylightSavings='Y',true,false) AS boolean)	AS hasDaylightSavings,
  DeliOrderPhone AS deliPhoneNumber,
  TRY_CAST('US' AS string) AS country,
  ARRAY['CURBSIDE','PHARMACY','GAS','BAKERY','DELI','DELIVERY','PHARMACY_DRIVE'] AS serviceTypes ,
  Tenants AS tenants,
  CAST(IF(CurbsideAllow = 'Y',TRUE,FALSE) AS boolean) AS curbsideEligibility,
  CAST('Mi9' AS string) AS curbsidePartner,
  row(row(AurusMerchantId,AurusTerminalId) ) as payment
  FROM
    `private.digital.stores.storeinfo-raw`
WHERE UnitId IS NOT NULL AND UnitId > 0)
WHERE row_num=1 -- for checking duplication rows
);
