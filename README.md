# amazon_any_csv_processor

steps

1. place csvs from ZonAsin into "csv_from_zon" folder

2. run python any_zon_csv.py

3. use csvs from "csv_merchant_url" and "csv_product_url" to get data from Octoparse

4. save data from Octoparse in "csv_merchant_octo" and "csv_product_octo" folder respectively

5. run python concantenate.py

6. run python create_blacklist_csv.py

7. run python create_whitelist_csv.py

8. run python create_whitelist_ASIN.py

9. upload csv from "white_list_ASINs" folder into AIS/ATS



