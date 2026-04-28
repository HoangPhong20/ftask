from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    broadcast,
    coalesce as spark_coalesce,
    col,
    count as spark_count,
    date_format,
    dayofmonth,
    dayofweek,
    hash,
    lit,
    make_date,
    month,
    quarter,
    sum as spark_sum,
    to_date,
    trim,
    upper,
    weekofyear,
    when,
    year,
)

logger = logging.getLogger("transform")


def build_flexi_daily(df_flexi_pg: DataFrame, enable_salt_agg: bool, salt_buckets: int, parse_nullable_double) -> DataFrame:
    df_flexi_prepared = (
        df_flexi_pg.select("charging_id", "record_type", "duration", "year", "month", "day")
        .withColumn("usage_date", make_date(col("year"), col("month"), col("day")))
        .withColumn("call_type_code", when(col("record_type").isNull() | (trim(col("record_type")) == ""), lit("UNKNOWN")).otherwise(upper(trim(col("record_type")))))
        .withColumn("used_duration_num", parse_nullable_double("duration"))
        .where(col("usage_date").isNotNull())
    )

    if enable_salt_agg:
        logger.info("Using salted 2-phase aggregation for skew mitigation: buckets=%s", salt_buckets)
        return (
            df_flexi_prepared.withColumn("salt", (hash(col("charging_id")) % lit(salt_buckets)).cast("int"))
            .groupBy("usage_date", "call_type_code", "salt")
            .agg(spark_count("*").alias("event_count"), spark_sum("used_duration_num").alias("total_used_duration"))
            .groupBy("usage_date", "call_type_code")
            .agg(spark_sum("event_count").alias("event_count"), spark_sum("total_used_duration").alias("total_used_duration"))
        )

    logger.info("Using single-phase aggregation to avoid extra shuffle from salting")
    return df_flexi_prepared.groupBy("usage_date", "call_type_code").agg(
        spark_count("*").alias("event_count"), spark_sum("used_duration_num").alias("total_used_duration")
    )


def build_dim_date(df_flexi_daily: DataFrame) -> DataFrame:
    return (
        df_flexi_daily.select("usage_date")
        .dropna(subset=["usage_date"])
        .dropDuplicates(["usage_date"])
        .withColumn("date_key", date_format(col("usage_date"), "yyyyMMdd").cast("int"))
        .withColumn("full_date", col("usage_date"))
        .withColumn("day_of_month", dayofmonth(col("usage_date")).cast("smallint"))
        .withColumn("month_of_year", month(col("usage_date")).cast("smallint"))
        .withColumn("quarter_of_year", quarter(col("usage_date")).cast("smallint"))
        .withColumn("year_number", year(col("usage_date")).cast("int"))
        .withColumn("week_of_year", weekofyear(col("usage_date")).cast("smallint"))
        .withColumn("day_name", date_format(col("usage_date"), "E"))
        .withColumn("month_name", date_format(col("usage_date"), "MMM"))
        .withColumn("is_weekend", when(dayofweek(col("usage_date")).isin(1, 7), lit(True)).otherwise(lit(False)))
        .select("date_key", "full_date", "day_of_month", "month_of_year", "quarter_of_year", "year_number", "week_of_year", "day_name", "month_name", "is_weekend")
    )


def build_dim_call_type(df_flexi_daily: DataFrame) -> DataFrame:
    return (
        df_flexi_daily.select("call_type_code")
        .dropna(subset=["call_type_code"])
        .dropDuplicates(["call_type_code"])
        .withColumn("call_type_name", col("call_type_code"))
        .withColumn("is_active", lit(True))
        .select("call_type_code", "call_type_name", "is_active")
    )


def build_fact_usage_daily(df_flexi_daily: DataFrame, df_dim_call_type: DataFrame, call_type_broadcast_threshold: int) -> DataFrame:
    df_fact_call_type_codes = df_flexi_daily.select("call_type_code").dropDuplicates(["call_type_code"])
    sampled_codes = df_fact_call_type_codes.limit(call_type_broadcast_threshold + 1).count()
    should_broadcast_call_type = sampled_codes <= call_type_broadcast_threshold

    df_dim_call_type_filtered = df_dim_call_type.join(df_fact_call_type_codes, "call_type_code", "inner").dropDuplicates(["call_type_code"])
    if should_broadcast_call_type:
        logger.info("Broadcast dim_call_type enabled: unique_call_types=%s threshold=%s", sampled_codes, call_type_broadcast_threshold)
        df_dim_call_type_filtered = broadcast(df_dim_call_type_filtered)
    else:
        logger.info("Broadcast dim_call_type disabled: unique_call_types>%s", call_type_broadcast_threshold)

    return (
        df_flexi_daily.withColumn("date_key", date_format(col("usage_date"), "yyyyMMdd").cast("int"))
        .join(df_dim_call_type_filtered.select("call_type_key", "call_type_code"), "call_type_code", "left")
        .withColumn("call_type_key", col("call_type_key").cast("long"))
        .withColumn("total_used_duration", spark_coalesce(col("total_used_duration"), lit(0.0)))
        .select("date_key", "call_type_key", "call_type_code", "event_count", "total_used_duration")
        .where(col("date_key").isNotNull() & col("call_type_key").isNotNull())
    )


def build_usage_summary_daily(df_fact_usage_daily_curated: DataFrame) -> DataFrame:
    return (
        df_fact_usage_daily_curated.groupBy("usage_date", "call_type_key", "call_type_code")
        .agg(spark_sum("event_count").alias("event_count"), spark_sum("total_used_duration").alias("total_used_duration"))
        .select("usage_date", "call_type_key", "call_type_code", "event_count", "total_used_duration")
    )


def to_curated_fact(df_fact_usage_daily: DataFrame) -> DataFrame:
    return (
        df_fact_usage_daily.withColumn("usage_date", to_date(col("date_key").cast("string"), "yyyyMMdd"))
        .select("date_key", "call_type_key", "usage_date", "call_type_code", "event_count", "total_used_duration")
    )
