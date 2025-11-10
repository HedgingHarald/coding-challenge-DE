# The Sample data

## customer 1001 & 1002

**ERP: cosmos** 

Data info

### 00_deliveries 
These files contain delivery data for multiple stores and products  over a range of dates. Each daily file contains multiple dates also from the past to be able to catch changes made in the ERP System. 
The file name indicates the date and time of data extraction.
- LI-Menge : delivery quantity 
- Datum : delivery date 
- Kunde_Nummer : store number 
- ArtNr : product number 
- Lieferzeit: delivery time (some products are delivered twice a day)

### 00_sales
These files contain sales data for multiple stores  and products over a range of dates. Each daily file contains multiple dates also from the past to be able to catch corrections made in the ERP System for specific days – products and stores.
The file name indicates the date and time of data extraction. 
- VK-Menge : sales quantity 
- Datum : sale date 
- Kunde : store number 
- Artikel : product number 
- VK-Betrag : revenue in Euro


### 00_products 
These files contain product information data for multiple products. They are daily updated to get the most recent product settings of the customers.
The file name indicates the date and time of data extraction.
- ArtNr: product number 
- Artikelgruppe: product family 
- Preis: product price 
- Bezeichnung: product name 
- Mindestbestellmenge: Minimum order quantity

### 00_stores 
These files contain product information data for multiple stores.They are daily updated to get the most recent stores settings of the customers.
The file name indicates the date and time of data extraction. 
- Nummer: store number 
- Straße: store name 
- PLZ: postal code 
- Ort: city 
- Land: country 
- Bundesland: state

## customer 1003

**ERP: galaxy** 
Data info

### 00_deliveries_sales
This file contains delivery and sales data for multiple stores ("Filialen") and products ("ArtikelNummer") over a range of dates.
The file name indicates the date and time of data extraction.

Entry Fields: 

-  ArtikelHistory 

Contains one or more objects, each representing a product's delivery and sales data for the given store and date.

    - ArtikelNummer : The product number (e.g., "1070").

    - Kundenbestellmenge : The quantity preordered by clients for this article on this date and store.

    - LieferNummer : delivery time (some products might be delivered twice a day)

    - Liefermenge : delivery quantity

    - UhrzeitErsterVk : Time of the first sale for this article (format: "HH:MM:SS").

    - UhrzeitLetzterVk : Time of the last sale for this article (format: "HH:MM:SS").

    - Verkaufsmenge : sales quantity

- Datum : sale date

- FilialNummer : store number
 
### 00_prices
This file contains product pricing information for articles available in the stores.
It is designed to provide a mapping between product numbers and their respective sales prices.
The file name indicates the date and time of data extraction.

Entry Fields:

    - ArtikelNummer : product number 

    - Artikelpreis : sales price of the product in Euros

### 00_products
This file contains master data for products available in the stores. It provides key attributes for each article, such as name, group, unit, and minimum order quantities.
The file name indicates the date and time of data extraction.

Entry Fields:

    - ArtikelHaltbarTage : number of days the product can be sold including the day of the delivery

    - ArtikelMasseinheit : Unit of measure for the product (e.g., "Stk" for Stück/piece)

    - ArtikelName : Name of the article 

    - ArtikelNummer : product number

    - Artikelgruppe : Product group 

    - BestellMindestMasseinheit : Minimum order unit (e.g., "Stk" for Stück/piece)

    - BestellMindestEinheit : Minimum order quantity

### 00_stores 
This file contains master data for the stores (Filialen) participating in the analysis. It provides key information such as store number, name, and address.
The file name indicates the date and time of data extraction.

Entry Fields:

    - FilialAnschrift : The full address of the store, with order street name and number, postal code, city, country, and state. Line breaks (\n) separate address components

    - FilialName (str): store name 

    - FilialNummer : store number


## Mapping Tables: 

**01_MappingProduct**: Maps customer product number to internal id_product

01_MappingProduct

id_product 	int

number_product 	int

**01_MappingStore**:  Maps customer store number to internal id_store

01_MappingStore

id_store 	int

number_store 	int

The internal ids are 9 digits long with the first four digits beeing the customer_number
(e.g. 1001XXXXX for id_product, 10019XXXX for id_store).
