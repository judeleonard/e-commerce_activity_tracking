# Assessment

## Content
- Archecture Overview
- Technologies and Functions
- Pipeline Explanation Task by Task
- Real-time pipeline Demo

## Architecture Overview
![]()

## Technologies and Functions

- `Postgres` Serves as the Data Warehouse
- `Amazon S3` serves as the Data Lake
- `Python` for data extractions and manipulation
- `SQL` data transformation
- `Airflow` orchestration and for running Cron Jobs
- `Docker` containerization

## Pipeline Explanation Task by Task

- ` `download_data_from_s3_task`
This task triggers a bash command that downloads all the staging data files from s3 and saves them to a local directly for staging

- `order_file_sensor_task`
This task is triggered once the `order.csv` file is present in the local. Which means if the download is not completed the pipeline would not proceed. This will serve as an additional check since we wouldn't know when the files will arrive in the directory.

- `review_file_sensor_task'
This task is triggered once the `reviews.csv` file is present in the download directory

- `shipment_del_file_sensor_task` 
This task is triggered once the `shipment_deliveries.csv` file is present in the download directory

- `creating_orders_staging_task`
This task creates the `orders_staging` table with all the key constraints associated with it in the data warehouse staging schema

- creating_reviews_staging_task
This task creates the `reviews_staging` table with all the key constraints associated with it in the data warehouse staging schema

- `creating_shipment_staging_task`
This task creates the `shipment_staging` table with all the key constraints associated with it in the data warehouse staging schema

- `creating_shipment_staging_task`
This task creates the `shipment_staging` table with all the key constraints associated with it in the data warehouse staging schema

- `creating_agg_public_holiday_task`
This task creates the `agg_publicholiday` table with all the key constraints associated with it in the data warehouse analytics schema

- `creating_agg_shipment_task`
This task creates the `agg_shipment_delivery` table with all the key constraints associated with it in the data warehouse analytics schema

- `creating_best_performing_products_task`
This task creates the `best_performing_product` table with all the key constraints associated with it in the data warehouse analytics schema

- `loading_order_staging_task`
This task copies the `order.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

- `loading_reviews_staging_task`
This task copies the `reviews.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

- `loading_shipment_delivery_staging_task`
This task copies the `shipment.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

- `loading_agg_order_public_holiday_task`





