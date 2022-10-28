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
This task downloads all the staging data files from s3 and saves them to a local directly for staging

- `order_file_sensor_task`
This task is triggered once the `order.csv` file is present in the local. Which means if the download is not completed the pipeline would not proceed. This will serve as an additional check since we wouldn't know when the files will arrive in the directory.

- `review_file_sensor_task'
This task is triggered once the `reviews.csv` file is present in the download directory

- `shipment_del_file_sensor_task` 
This task is triggered once the `shipment_deliveries.csv` file is present in the download directory

- `

