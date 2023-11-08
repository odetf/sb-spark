import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, from_json, lit, struct, sum, to_json, when, window}
import org.apache.spark.sql.streaming.Trigger

object agg {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder.appName("lab04b").getOrCreate()

    import spark.implicits._

    val streamDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "olga_tarasenko")
      .load()

    val dataFromStream = streamDF.select($"value".cast("string").as[String])

    val schema = StructType(Seq(
      StructField("category", StringType, true),
      StructField("event_type", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", StringType, true),
      StructField("timestamp", LongType, true),
      StructField("uid", StringType, true)
    ))

    val aggregatedDF =

      dataFromStream.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")
        .withColumn("date", ($"timestamp" / 1000).cast(TimestampType))
        .groupBy(window(col("date"), "1 hour"))
        .agg(
          sum(when($"event_type" === "buy", col("item_price")).otherwise(0)).alias("revenue"),
          sum(when($"uid".isNotNull, 1).otherwise(0)).alias("visitors"),
          sum(when($"event_type" === "buy", 1).otherwise(0)).alias("purchases")
        )
        .withColumn("aov", $"revenue" / $"purchases")
        .withColumn("start_ts", $"window.start".cast("long"))
        .withColumn("end_ts", $"window.end".cast("long")).drop(col("window"))
        .selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")

    val writeToKafka = aggregatedDF
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", "lab04b_checkpoint_olga_tarasenko")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "olga_tarasenko_lab04b_out")
      .outputMode("update")
      .start()

    writeToKafka.awaitTermination()

  }


}
