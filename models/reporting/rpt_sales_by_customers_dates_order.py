import snowflake.snowpark.functions as F
import pandas as pd
import holidays
 
def is_holiday(orderdate):
 
    french_holidays = holidays.France()
 
    is_holiday = (orderdate in french_holidays)
 
    return is_holiday
 
def avgordervalue(totalorders,totalsales):
 
    return totalsales/totalorders
 
def model(dbt,session):
 
    dbt.config(materialized='table', schema = 'reporting_dev', packages = ['holidays'])
 
    fct_orders_df= dbt.ref("fct_orders")
 
    dim_customers_df= dbt.ref("dim_customers")
 
    fct_orders_aggregate = (
                                fct_orders_df
                                .group_by("customerid")
                                .agg(
                                    F.min(F.col("orderdate")).alias('first_order_date'),
                                    F.max(F.col("orderdate")).alias('recent_order_date'),
                                    F.countDistinct(F.col("orderid")).alias('total_orders'),
                                    F.countDistinct(F.col("productid")).alias("total_products"),
                                    F.sum(F.col("quantity")).alias('total_quantity'),
                                    F.sum(F.col("linesalesamount")).alias('total_sales'),
                                    F.sum(F.col("margin")).alias('average_margin')  
                                )
    )
    fct_orders_customers_df =        (
                                dim_customers_df
                                .join(fct_orders_aggregate, dim_customers_df.customerid == fct_orders_aggregate.customerid, 'left')
                                .select
                                (
                                    dim_customers_df.companyname,
                                    dim_customers_df.contactname,
                                    dim_customers_df.city,
                                    dim_customers_df.country,
                                    fct_orders_aggregate.first_order_date,
                                    fct_orders_aggregate.recent_order_date,
                                    fct_orders_aggregate.total_orders,
                                    fct_orders_aggregate.total_products,
                                    fct_orders_aggregate.total_quantity,
                                    fct_orders_aggregate.total_sales,
                                    fct_orders_aggregate.average_margin
                                )
    )
    final_df = fct_orders_customers_df.withColumn("average_order_value",avgordervalue(fct_orders_customers_df["total_orders"],fct_orders_customers_df["total_sales"]))
    final_df =  final_df.filter(F.col('FIRST_ORDER_DATE').isNotNull())
 
    final_df = final_df.to_pandas()
 
    final_df["IS_FIRST_ORDER_DATE_ON_HOLIDAY"] = final_df["FIRST_ORDER_DATE"].apply(is_holiday)
    return final_df