import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.net.{URL, URLDecoder}
import scala.util.Try

class lab02 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("lab02").getOrCreate()

    val usersSchema = StructType(List(StructField("autousers", ArrayType(StringType, containsNull = false))))
    val users = spark.read.schema(usersSchema).json("/labs/laba02/autousers.json")

    val autousers = users.withColumn("autousers", explode(users.col("autousers")))

    val logSchema = StructType(List(
      StructField("uid", StringType),
      StructField("ts", StringType),
      StructField("url", StringType)
    )
    )
    val logsWithoutDomen = spark.read.schema(logSchema).option("delimiter", "\t").csv("/labs/laba02/logs")
      .filter("url is not null")
      .filter("uid is not null")
      .filter("url != '-' and uid != '-'")
      .select(col("uid"), deleteWWW(decodeUrlAndGetDomain(col("url"))).alias("url"))

    val dfForAnalysis = logsWithoutDomen.join(autousers, logsWithoutDomen("uid") === autousers("autousers"), "left_outer")
      .select(col("url"), expr("CASE WHEN autousers IS NULL THEN 0 ELSE 1 END").alias("auto-flag"))

    val num_of_total_auto = dfForAnalysis.select(sum(col("auto-flag")).alias("total_auto"))

    val aggrLogs = dfForAnalysis.groupBy("url")
      .agg(sum(col("auto-flag")).alias("num_of_autousers"),
        count(col("url")).alias("num_of_url_repeats"))

    aggrLogs.crossJoin(num_of_total_auto)
      .select(col("url"), ((col("num_of_autousers") * col("num_of_autousers")).divide(col("num_of_url_repeats") * col("total_auto"))).alias("relevance"),
        col("num_of_autousers"), col("num_of_url_repeats"))
      .orderBy(desc("relevance"), asc("url"))
      .select(col("url").alias("domain"), col("relevance"))
      .limit(200)
      .coalesce(1)
      .write.mode("overwrite").option("delimiter", "\t").csv("laba02/laba02_domains.txt")

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
