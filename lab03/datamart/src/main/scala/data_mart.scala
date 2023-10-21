import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try

class data_mart {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host", "10.0.0.31")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    val cassandraDF = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "clients", "keyspace" -> "labdata"))
      .load()

    val elasticDF = spark.read.format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> "9200",
        "es.nodes" -> "10.0.0.31",
        "es.net.ssl" -> "false"))
      .load("visits")

    val aggrElasticDF = elasticDF.filter("uid is not null").select($"uid", expr("'shop_' || lower(translate(category, '- ', '__'))").alias("category"))
      .groupBy($"uid").pivot("category").agg(count("uid")).na.fill(0)

    val websiteLogs = spark.read.json("hdfs:///labs/laba03/weblogs.json")

    val websiteLogsCleared = websiteLogs.select($"uid", explode($"visits").alias("visits"))
      .select($"uid",
        col("visits").getItem("timestamp").alias("timestamp"),
        col("visits").getItem("url").alias("url"))
      .select(col("uid"), col("timestamp"), col("url"))
      .filter("uid is not null")

    val websiteLogsWithoutDomains = websiteLogsCleared.select($"uid", deleteWWW(decodeUrlAndGetDomain($"url")).alias("domain"))

    val categories: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
      .option("dbtable", "domain_cats")
      .option("user", "olga_tarasenko")
      .option("password", "Ywngsze1")
      .option("driver", "org.postgresql.Driver")
      .load()

    val joinCondition = col("logs.domain") === col("categories.domain")
    val websiteLogsAggr = websiteLogsWithoutDomains.alias("logs").join(categories.alias("categories"), joinCondition, "inner")
      .select($"uid",
        expr("'web_' || lower(translate(categories.category, '- ', '__'))").alias("category"))
      .groupBy($"uid")
      .pivot("category")
      .agg(count("uid"))
      .na.fill(0)

    val finalDataMart = cassandraDF.select($"uid", $"gender", when($"age" < 25, "18-24")
      .when($"age" < 35, "25-34")
      .when($"age" < 45, "35-44")
      .when($"age" < 55, "45-54")
      .when($"age" >= 55, ">=55").alias("age_cat"))
      .alias("clients")
      .join(aggrElasticDF.alias("shop_cat"), col("clients.uid") === col("shop_cat.uid"), "left")
      .join(websiteLogsAggr.alias("web_cat"), col("clients.uid") === col("web_cat.uid"), "left")
      .drop($"shop_cat.uid")
      .drop($"web_cat.uid")

    finalDataMart.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.0.0.31:5432/olga_tarasenko")
      .option("dbtable", "clients")
      .option("user", "olga_tarasenko")
      .option("password", "Ywngsze1")
      .option("driver", "org.postgresql.Driver")
      .option("truncate", value = true)
      .mode("overwrite")
      .save()

  }


  def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
  })

  def deleteWWW: UserDefinedFunction = udf((url: String) => {
    if (url.startsWith("www.") == true) {
      url.slice(4, url.length)
    }
    else {
      url
    }
  })
}
