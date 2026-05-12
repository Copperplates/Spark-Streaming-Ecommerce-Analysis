import argparse
import os
import shutil
import time
from datetime import datetime
from datetime import timedelta

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.functions import col
from pyspark.sql.functions import count
from pyspark.sql.functions import from_json
from pyspark.sql.functions import lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import when
from pyspark.sql.types import BooleanType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType

from realtime_reporting import append_csv
from realtime_reporting import ensure_output_layout
from realtime_reporting import render_dashboard
from realtime_reporting import write_csv


DEFAULT_OUTPUT_DIR = "/opt/spark-results/realtime"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming processor for e-commerce events."
    )
    parser.add_argument(
        "--source-type",
        default="files",
        choices=["files", "kafka"],
        help="Use file streaming by default; Kafka is optional.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka-broker:29092",
        help="Kafka bootstrap servers inside Docker network.",
    )
    parser.add_argument(
        "--topic",
        default="ecommerce-transactions",
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory mounted from the host for saved results.",
    )
    parser.add_argument(
        "--input-dir",
        default="/opt/spark-data/realtime_input",
        help="Directory used by file-stream mode.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=os.path.join(DEFAULT_OUTPUT_DIR, "checkpoints", "processor"),
        help="Checkpoint directory for Structured Streaming state.",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=5,
        help="Micro-batch trigger interval in seconds.",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=5,
        help="Stop automatically after this many non-empty batches.",
    )
    parser.add_argument(
        "--idle-timeout-seconds",
        type=int,
        default=12,
        help="Stop after this many seconds without a new non-empty batch.",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["earliest", "latest"],
        help="Kafka starting offsets for the stream.",
    )
    parser.add_argument(
        "--reset-output",
        action="store_true",
        help="Delete the realtime output directory before processing.",
    )
    return parser.parse_args()


def build_schema():
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("gender", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("city", StringType(), True),
            StructField("membership_type", StringType(), True),
            StructField("total_spend", DoubleType(), True),
            StructField("items_purchased", IntegerType(), True),
            StructField("average_rating", DoubleType(), True),
            StructField("discount_applied", BooleanType(), True),
            StructField("days_since_last_purchase", IntegerType(), True),
            StructField("satisfaction_level", StringType(), True),
        ]
    )


def safe_reset_output(output_dir, checkpoint_dir):
    realtime_root = os.path.abspath(output_dir)
    if os.path.basename(realtime_root) != "realtime":
        raise ValueError(f"Refusing to delete unexpected output directory: {output_dir}")

    if os.path.exists(realtime_root):
        shutil.rmtree(realtime_root)

    checkpoint_root = os.path.abspath(checkpoint_dir)
    if checkpoint_root.startswith(realtime_root):
        os.makedirs(checkpoint_root, exist_ok=True)


def create_spark_session(source_type):
    builder = SparkSession.builder.appName("EcommerceRealtimeProcessor").config(
        "spark.sql.shuffle.partitions", "4"
    )
    if source_type == "kafka":
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2",
        )
    return builder.getOrCreate()


def enrich_stream(parsed_df):
    return (
        parsed_df.withColumn("event_time", to_timestamp("event_time"))
        .withColumn(
            "is_at_risk",
            when(
                (col("days_since_last_purchase") > 30)
                & (
                    (col("satisfaction_level") == lit("Unsatisfied"))
                    | (col("average_rating") < lit(3.8))
                ),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "high_value_order",
            when(col("total_spend") >= 1000.0, lit(1)).otherwise(lit(0)),
        )
    )


def main():
    args = parse_args()
    if args.reset_output:
        safe_reset_output(args.output_dir, args.checkpoint_dir)

    ensure_output_layout(args.output_dir)
    spark = create_spark_session(args.source_type)
    state = {
        "processed_batches": 0,
        "cumulative_event_count": 0,
        "should_stop": False,
        "last_batch_completed_at": None,
    }

    if args.source_type == "kafka":
        raw_stream_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )
        parsed_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) AS json_value")
        parsed_stream_df = parsed_stream_df.select(
            from_json(col("json_value"), build_schema()).alias("event")
        ).select("event.*")
    else:
        os.makedirs(args.input_dir, exist_ok=True)
        parsed_stream_df = (
            spark.readStream.schema(build_schema()).json(args.input_dir)
        )
    enriched_stream_df = enrich_stream(parsed_stream_df)

    def process_batch(batch_df, batch_id):
        event_count = batch_df.count()
        if event_count == 0:
            return

        state["processed_batches"] += 1
        state["cumulative_event_count"] += event_count

        ordered_df = batch_df.orderBy("event_time", "event_id")
        latest_events_pdf = ordered_df.toPandas()
        latest_events_pdf["event_time"] = latest_events_pdf["event_time"].astype(str)

        batch_events_path = os.path.join(
            args.output_dir, "events", f"batch_{int(batch_id):03d}.csv"
        )
        write_csv(latest_events_pdf, batch_events_path)
        write_csv(latest_events_pdf, os.path.join(args.output_dir, "latest_events.csv"))
        append_csv(latest_events_pdf, os.path.join(args.output_dir, "stream_events.csv"))

        city_summary_pdf = (
            ordered_df.groupBy("city")
            .agg(
                count("*").alias("event_count"),
                spark_round(avg("total_spend"), 2).alias("avg_total_spend"),
                spark_sum("items_purchased").alias("total_items_purchased"),
            )
            .orderBy("city")
            .toPandas()
        )
        write_csv(city_summary_pdf, os.path.join(args.output_dir, "city_summary_latest.csv"))

        membership_summary_pdf = (
            ordered_df.groupBy("membership_type")
            .agg(
                count("*").alias("event_count"),
                spark_round(avg("total_spend"), 2).alias("avg_total_spend"),
                spark_round(avg("average_rating"), 2).alias("avg_rating"),
                spark_sum("high_value_order").alias("high_value_orders"),
            )
            .orderBy("membership_type")
            .toPandas()
        )
        write_csv(
            membership_summary_pdf,
            os.path.join(args.output_dir, "membership_summary_latest.csv"),
        )

        risk_pdf = (
            ordered_df.filter(col("is_at_risk"))
            .select(
                "event_time",
                "customer_id",
                "city",
                "membership_type",
                "total_spend",
                "average_rating",
                "days_since_last_purchase",
                "satisfaction_level",
            )
            .orderBy(col("days_since_last_purchase").desc(), col("average_rating").asc())
            .toPandas()
        )
        if not risk_pdf.empty:
            risk_pdf["event_time"] = risk_pdf["event_time"].astype(str)
        write_csv(risk_pdf, os.path.join(args.output_dir, "at_risk_customers_latest.csv"))

        metrics_row = pd.DataFrame(
            [
                {
                    "processed_at": datetime.now().isoformat(timespec="seconds"),
                    "batch_id": int(batch_id),
                    "processed_batches": state["processed_batches"],
                    "event_count": event_count,
                    "cumulative_event_count": state["cumulative_event_count"],
                    "total_revenue": round(
                        float(latest_events_pdf["total_spend"].sum()), 2
                    ),
                    "avg_spend": round(
                        float(latest_events_pdf["total_spend"].mean()), 2
                    ),
                    "avg_rating": round(
                        float(latest_events_pdf["average_rating"].mean()), 2
                    ),
                    "discounted_orders": int(
                        latest_events_pdf["discount_applied"].astype(bool).sum()
                    ),
                    "high_value_orders": int(
                        latest_events_pdf["high_value_order"].sum()
                    ),
                    "at_risk_count": int(len(risk_pdf)),
                }
            ]
        )
        append_csv(metrics_row, os.path.join(args.output_dir, "kpi_history.csv"))
        write_csv(metrics_row, os.path.join(args.output_dir, "latest_batch_metrics.csv"))

        render_dashboard(args.output_dir)
        print(
            "Processed batch "
            f"{state['processed_batches']}/{args.max_batches} "
            f"with {event_count} events. "
            f"Results saved to {args.output_dir}."
        )
        state["last_batch_completed_at"] = datetime.now()

        if state["processed_batches"] >= args.max_batches:
            state["should_stop"] = True

    query = (
        enriched_stream_df.writeStream.foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    try:
        while query.isActive:
            query.awaitTermination(5)
            if (
                state["last_batch_completed_at"] is not None
                and datetime.now() - state["last_batch_completed_at"]
                >= timedelta(seconds=args.idle_timeout_seconds)
            ):
                state["should_stop"] = True
            if state["should_stop"] and query.isActive:
                query.stop()
                break
            time.sleep(1)
    finally:
        render_dashboard(args.output_dir)
        spark.stop()


if __name__ == "__main__":
    main()
