import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.CountVectorizerModel


object features {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder().appName("laba06").getOrCreate()

    import spark.implicits._

    val toArrayUDF = udf((v: org.apache.spark.ml.linalg.Vector) => v.toDense.toArray)

    val weblogs = spark.read.parquet("/user/olga.tarasenko/users-items/20200429")

    val usersItemsMatrix = spark.read.parquet("/user/olga.tarasenko/users-items/20200429")

    val weblogsDomains = weblogs.select('uid, explode('visits).alias("visit"))
      .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .withColumn("timestamp", $"visit.timestamp" / 1000)
      .withColumn("hour", from_unixtime($"timestamp", "H"))
      .withColumn("dayOfWeek", concat(lit("web_day_"), lower(date_format('timestamp.cast("timestamp"), "E"))))
      .filter('domain.isNotNull)
      .filter('uid.isNotNull)
      .select('uid, 'domain, 'dayOfWeek, 'hour)

    val visitsByDayOfWeek = weblogsDomains
      .groupBy('uid)
      .pivot('dayOfWeek)
      .agg(count("domain").alias("num_of_domains"))
      .na.fill(0)

    val visitsByHours = weblogsDomains
      .withColumn("hour", concat(lit("web_hour_"), 'hour))
      .groupBy('uid)
      .pivot('hour).agg(count("domain").alias("num_of_domains"))
      .na.fill(0)

    val visitsHoursFraction = weblogsDomains
      .withColumn("total_visits", count('domain).over(Window.partitionBy('uid)))
      .withColumn("web_fraction_work_hours", sum(when('hour >= lit(9) && 'hour < lit(18), 1).otherwise(0)).over(Window.partitionBy('uid)) / 'total_visits)
      .withColumn("web_fraction_evening_hours", sum(when('hour >= lit(18) && 'hour < lit(24), 1).otherwise(0)).over(Window.partitionBy('uid)) / 'total_visits)
      .select('uid, 'web_fraction_work_hours, 'web_fraction_evening_hours).distinct()

    val vectorizedDF = weblogsDomains.groupBy('uid).agg(collect_list('domain).alias("domains"))
    val topWebsites = weblogsDomains.groupBy('domain)
      .agg(count('uid).alias("num_of_visits"))
      .orderBy(desc("num_of_visits"), asc("domain"))
      .limit(1000)
      .orderBy(asc("domain"))
      .select('domain).as[String].collect

    val countVecModel = new CountVectorizerModel(topWebsites)
      .setInputCol("domains")
      .setOutputCol("domain_features")
      .transform(vectorizedDF)
      .withColumn("domain_features", toArrayUDF('domain_features))
      .drop('domains)

    val featuresDF = visitsHoursFraction.join(visitsByDayOfWeek, Seq("uid"), "inner")
      .join(visitsByHours, Seq("uid"), "inner")
      .join(countVecModel, Seq("uid"), "inner")
      .join(weblogs, Seq("uid"), "inner")

    featuresDF.write.mode("overwrite").parquet("/user/olga.tarasenko/features")

  }

}
