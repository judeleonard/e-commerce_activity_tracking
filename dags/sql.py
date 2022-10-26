
create_orders_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.orders_staging;
    CREATE TABLE judendu4707_staging.orders_staging(
	order_id INT NOT NULL GENERATED ALWAYS AS IDENTITY, 
	customer_id INT NOT NULL, 
	order_date DATE NOT NULL, 
	product_id INT NOT NULL,
	unit_price INT NOT NULL, 
	quantity INT NOT NULL, 
	total_price INT NOT NULL,
    PRIMARY KEY(order_id)
    );

""")



create_reviews_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.reviews_staging;
    CREATE TABLE judendu4707_staging.reviews_staging(
    review INT, 
	product_id INT NOT NULL
    );

""")

create_shipment_delivery_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.shipment_del_staging;
    CREATE TABLE judendu4707_staging.shipment_del_staging(
    shipment_id INT NOT NULL PRIMARY KEY, 
	order_id INT NOT NULL, 
	shipment_date DATE NULL, 
	delivery_date DATE NULL
    );

""")

create_agg_order_public_holiday_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.agg_public_holiday;
    CREATE TABLE judendu4707_analytics.agg_public_holiday(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    tt_order_hol_jan INT NOT NULL,
    tt_order_hol_feb INT NOT NULL,
    tt_order_hol_mar INT NOT NULL,
    tt_order_hol_apr INT NOT NULL,
    tt_order_hol_may INT NOT NULL,
    tt_order_hol_jun INT NOT NULL,
    tt_order_hol_jul INT NOT NULL,
    tt_order_hol_aug INT NOT NULL,
    tt_order_hol_sep INT NOT NULL,
    tt_order_hol_oct INT NOT NULL,
    tt_order_hol_nov INT NOT NULL,
    tt_order_hol_dec INT NOT NULL,
    PRIMARY KEY(ingestion_date));

""")


create_agg_shipment_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.agg_shipments;
    CREATE TABLE judendu4707_analytics.agg_shipments(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    tt_late_shipments INT NOT NULL,
    tt_undelivered_items INT NOT NULL,
    PRIMARY KEY(ingestion_date));

""")

create_best_performing_product_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.best_performing_product;
    CREATE TABLE judendu4707_analytics.best_performing_product(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    product_name VARCHAR(255)  NULL,
    most_ordered_day DATE  NULL,
    is_public_holiday boolean NULL,
    tt_review_point INT NULL,
    pct_one_star_review FLOAT NOT NULL,
    pct_two_star_review FLOAT NOT NULL,
    pct_three_star_review FLOAT NOT NULL,
    pct_four_star_review FLOAT NOT NULL,
    pct_five_star_review FLOAT NOT NULL,
    pct_early_shipments FLOAT NULL,
    pct_late_shipments FLOAT NULL,
    PRIMARY KEY(ingestion_date));

""")


drop_staging_tables = ("""
    DROP TABLE IF EXISTS judendu4707_staging.reviews_staging;
    DROP TABLE IF EXISTS judendu4707_staging.orders_staging;
    DROP TABLE IF EXISTS judendu4707_staging.shipment_del_staging;

""")


load_agg_order_public_holiday_table = ("""
    INSERT INTO judendu4707_analytics.agg_public_holiday(tt_order_hol_jan, 
                                                         tt_order_hol_feb, 
                                                         tt_order_hol_mar, 
                                                         tt_order_hol_apr, 
                                                         tt_order_hol_may, 
                                                         tt_order_hol_jun, 
                                                         tt_order_hol_jul,
                                                         tt_order_hol_aug,
                                                         tt_order_hol_sep,
                                                         tt_order_hol_oct,
                                                         tt_order_hol_nov,
                                                         tt_order_hol_dec)
    WITH public_hol_cte AS 
    (
        SELECT ot.order_date, ot.order_id, dd.calendar_dt, dd.month_of_the_year_num,
            dd.day_of_the_week_num, dd.working_day                                                                     
        FROM judendu4707_staging.orders_staging ot
        JOIN
        if_common.dim_dates AS dd
        ON dd.calendar_dt = ot.order_date 
    )

    SELECT
        SUM(CASE
               WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5) 
               AND (ph.month_of_the_year_num = 1 AND ph.working_day = false) THEN 1
               ELSE 0
	      END) AS "tt_order_hol_jan",

        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 2 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_feb",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 3 AND ph.working_day=false) THEN 1
                ELSE 0 
          END) AS "tt_order_hol_mar",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 4 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_apr",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 5 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_may",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 6 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_jun",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 7 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_jul",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 8 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_aug",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 9 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_sep",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 10 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_oct",
        SUM (CASE 
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 11 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_nov",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 12 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_dec"

    FROM public_hol_cte AS ph;
""")


load_agg_shipment_table = ("""
    INSERT INTO judendu4707_analytics.agg_shipments(tt_late_shipments, tt_undelivered_items)

    WITH dates_cte AS 
     (
     SELECT order_id, shipment_date, delivery_date,
 		(sd.shipment_date + 6) AS valid_date,
 		('2022-09-05'::date + 15) AS deadline_date
     FROM judendu4707_staging.shipment_del_staging AS sd
     )

SELECT 
	SUM(CASE 
 			WHEN ot.order_date <> d.shipment_date THEN 1
		    WHEN ot.order_date <> d.valid_date THEN 1
		    WHEN d.delivery_date <> d.shipment_date THEN 1
			WHEN d.delivery_date <> d.valid_date THEN 1
	   		ELSE 0
	END) as "tt_late_shipments",
	SUM(CASE 
 			WHEN ot.order_date <> '2022-09-05' THEN 1
			WHEN ot.order_date <> d.deadline_date THEN 1
			WHEN d.delivery_date IS NULL AND d.shipment_date IS NULL
			THEN 1
 			ELSE 0
 	END) as "tt_undelivered_items" 
	
FROM dates_cte d
INNER JOIN judendu4707_staging.orders_staging ot USING (order_id);
   
""")
    

load_best_performing_product = """
    INSERT INTO judendu4707_analytics.best_performing_product(pct_one_star_review,
                                                              pct_two_star_review,
                                                              pct_three_star_review,
                                                              pct_four_star_review,
                                                              pct_five_star_review)

 
   (SELECT 
        ROUND(
        100.0 * (
            SUM(CASE WHEN review = 1 THEN 1 ELSE 0 END)::DECIMAL / COUNT(review)
            ), 2) pct_one_star_review,
        ROUND(
        100.0 * (
            SUM(CASE WHEN review = 2 THEN 1 ELSE 0 END)::DECIMAL / COUNT(review)
            ), 2) pct_two_star_review,
        ROUND(
        100.0 * (
            SUM(CASE WHEN review = 3 THEN 1 ELSE 0 END)::DECIMAL / COUNT(review)
            ), 2) pct_three_star_review,
        ROUND(
        100.0 * (
            SUM(CASE WHEN review = 4 THEN 1 ELSE 0 END)::DECIMAL / COUNT(review)
            ), 2) pct_four_star_review,
        ROUND(
        100.0 * (
            SUM(CASE WHEN review = 5 THEN 1 ELSE 0 END)::DECIMAL / COUNT(review)
            ), 2) pct_five_star_review


    FROM judendu4707_staging.reviews_staging);

"""



#------- This query returns the result of the following fields; product_name, is_public_holiday, most_ordered_day, -----#

#     WITH transform_cte AS 
#     (
#         SELECT dd.calendar_dt, dd.day_of_the_week_num, dd.working_day, sd.shipment_date,
# 			(sd.shipment_date + 6) AS valid_date, sd.delivery_date, sd.order_id
# 		FROM if_common.dim_dates dd
# 		LEFT JOIN judendu4707_staging.shipment_del_staging AS sd
# 		ON sd.shipment_date=dd.calendar_dt
		
# 	)

 
#     SELECT
#        dp.product_name, ot.quantity, ot.product_id, dp.product_id, ot.order_id,
#         tc.day_of_the_week_num AS most_ordered_day,
#         tc.working_day AS is_public_holiday
#     FROM transform_cte tc
#     INNER JOIN judendu4707_staging.orders_staging ot USING (order_id)
#     INNER JOIN if_common.dim_products dp USING (product_id)
#     ORDER BY ot.quantity DESC
#     LIMIT 1;

#    (SELECT review,COUNT(*) AS "tt_review_point"
#     FROM judendu4707_staging.reviews_staging
#    GROUP BY review)


####################   CONSTRAINTS ###########################
 
# alter table judendu4707_staging.orders_staging ADD CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES if_common.dim_customers(customer_id);
# alter table judendu4707_staging.orders_staging ADD CONSTRAINT fk_product_id FOREIGN KEY(product_id) REFERENCES if_common.dim_products(product_id);
# alter table judendu4707_staging.reviews_staging ADD CONSTRAINT fk_product_id FOREIGN KEY(product_id) REFERENCES if_common.dim_products(product_id);
# alter table judendu4707_staging.shipment_del_staging ADD CONSTRAINT fk_orders FOREIGN KEY(order_id) REFERENCES judendu4707_staging.orders_staging(order_id);
