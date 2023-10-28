class filter {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.types._
  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._

  val spark = SparkSession.builder().appName("lab04").getOrCreate()

  val topic_name: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
  val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
  val output_dir_prefix: String = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  import spark.implicits._


  val kafkaParams = Map("kafka.bootstrap.servers" -> "spark-master-1:6667",
    "subscribe" -> topic_name,
    "startingOffsets" -> (if (offset.contains("earliest"))
      offset
    else {
      "{\"" + topic_name + "\":{\"0\":" + offset + "}}"
    }))
  val kafkaDF = spark.read.format("kafka").options(kafkaParams).load

  val jsonTransformedDF = spark.read.json(kafkaDF.select('value.cast("string")).as[String])
    .withColumn("date",
      date_format(from_unixtime($"timestamp" / 1000).cast("timestamp"), "yyyyMMdd").cast("string"))

  jsonTransformedDF.withColumn("p_date", $"date")
    .filter($"event_type" === "buy")
    .write
    .partitionBy("p_date")
    .json(output_dir_prefix + "/buy/")

  jsonTransformedDF.withColumn("p_date", $"date")
    .filter($"event_type" === "view")
    .write
    .partitionBy("p_date")
    .json(output_dir_prefix + "/view/")

  spark.stop()


}
