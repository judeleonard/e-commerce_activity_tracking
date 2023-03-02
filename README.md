# Data Engineering Technical Assessment

## Content

- Archecture Overview
- Technologies and Functions
- Pipeline Explanation Task by Task
- Real-time pipeline Demo
- Side Note

## Architecture Overview
![](https://github.com/judeleonard/Assessment-repo/blob/d2b/images/architecture.png)

## Technologies and Functions

- `Postgres` Serves as the Data Warehouse
- `Amazon S3` serves as the Data Lake
- `Python` for data extractions and loading
- `SQL` data transformation
- `Airflow` orchestration and for running Cron Jobs
- `Docker` containerization

## Pipeline Explanation Task by Task

#### `download_data_from_s3_task`

This task triggers a bash command that downloads all the staging data files from s3 and saves them to a local directly for staging

#### `order_file_sensor_task`

This task is triggered once the `order.csv` file is present in the local. Which means if the download is not completed the pipeline would not proceed. This will serve as an additional check since we wouldn't know when the files will arrive in the directory.

#### `review_file_sensor_task`

This task is triggered once the `reviews.csv` file is present in the download directory

#### `shipment_del_file_sensor_task`
 
This task is triggered once the `shipment_deliveries.csv` file is present in the download directory

#### `creating_orders_staging_task`

This task creates the `orders_staging` table with all the key constraints associated with it in the data warehouse staging schema

#### `creating_reviews_staging_task`

This task creates the `reviews_staging` table with all the key constraints associated with it in the data warehouse staging schema

#### `creating_shipment_staging_task`

This task creates the `shipment_staging` table with all the key constraints associated with it in the data warehouse staging schema

#### `creating_shipment_staging_task`

This task creates the `shipment_staging` table with all the key constraints associated with it in the data warehouse staging schema

#### `creating_agg_public_holiday_task`

This task creates the `agg_publicholiday` table with all the key constraints associated with it in the data warehouse analytics schema

#### `creating_agg_shipment_task`

This task creates the `agg_shipment_delivery` table with all the key constraints associated with it in the data warehouse analytics schema

#### `creating_best_performing_products_task`

This task creates the `best_performing_product` table with all the key constraints associated with it in the data warehouse analytics schema

#### `loading_order_staging_task`

This task copies the `order.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

#### `loading_reviews_staging_task`

This task copies the `reviews.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

#### `loading_shipment_delivery_staging_task`

This task copies the `shipment.csv` file from the local directory where it is downloaded for staging into the data warehouse staging schema

#### `loading_agg_order_public_holiday_task`

This task uses SQL operator to transform the data copied to data warehouse staging schema and loads the resultant query into the `agg_order_public_holiday` table in the data warehouse analytics schema

#### `loading_agg_shipment_task`

This task uses SQL operator to transform the data copied to data warehouse staging schema and loads the resultant query into the `agg_shipment` table in the data warehouse analytics schema

#### `loading_best_product_task`

This task uses SQL operator to transform the data copied to data warehouse staging schema and loads the resultant query into the `best_product` table in the data warehouse analytics schema

#### `data_quality_check_task`

This is an utility task that ensures the data loaded to our analytics schema must pass all criteria in terms of data reliability. Since our insight tables lives in the warehouse analytics schema, then data quality check is run on each of the tables once data transformation completed

#### `clean_up_task`

This task cleans the entire pipeline processes in preparation for the next run. Removes all the downloaded files from our local staging directory and also drops all the tables in our data warehouse staging schema since they are no longer useful once data transformation is completed. Although these staging tables are all set to be dropped upon next run.

#### `exporting_agg_public_holiday_task`

This task exports the `agg_public_holiday` table from our data warehouse analytics schema to an S3 bucket as a backup or archive file

#### `exporting_agg_shipments_task`

This task exports the `agg_shipment` table from our data warehouse analytics schema to an S3 bucket as a backup or archive file

#### `exporting_best_performing_product_task`

This task exports the `best_product` table from our data warehouse analytics schema to an S3 bucket as a backup or archive file


## Pipeline Demo
![](https://github.com/judeleonard/Assessment-repo/blob/d2b/images/pipeline.png)

## [See demo in real-time here](https://vimeo.com/765006630/457467d7d2)

It took roughly about 1:42 for the pipeline to reach completion. This was heavily impacted by my poor network as at the time I was taking this record.
With my strong internet connection it took  roughly about 1:10 for the pipeline to reach completion.

### Side Note
Other things I can stll improve on
- Modify the some of the SQL queries to make it even more robust and efficient
- Add more data quality checks to continously ensure we are getting the right data into our storage





