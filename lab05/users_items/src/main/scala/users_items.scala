import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import sys.process._

import org.apache.hadoop.fs.{FileSystem, Path}

object users_items {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("lab05").getOrCreate()

    val update: Integer = spark.sparkContext.getConf.get("spark.users_items.update").toInt
    val output_dir: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")
    val input_dir: String = spark.sparkContext.getConf.get("spark.users_items.input_dir")

    import spark.implicits._

    def find_max_date[_](df1: DataFrame, df2: DataFrame): String = {
      val max1 = df1.select(max('p_date)).collectAsList().get(0).mkString
      val max2 = df2.select(max('p_date)).collectAsList().get(0).mkString

      if (max1 < max2) {
        max2
      } else {
        max1
      }
    }

    def returnLastVersion(dir: String) = {
      if (s"${dir}" contains "file:/") {
        val correct_dir = dir.replace("file:", "")
        (s"ls ${correct_dir} -X -r".!!).split("\\n").map(_.trim).toList(0)
      }
      else {
        val hadoopConfiguration = spark.sparkContext.hadoopConfiguration
        val thisFs = FileSystem.get(hadoopConfiguration)
        thisFs.listStatus(new Path(dir))
          .map(x => x.getPath.toString)
          .sorted(Ordering.String.reverse)
          .head takeRight 8
      }
    }

    if (update == 0) {
      val views_df = spark.read.json(input_dir + "/view/").filter("uid is not null")
      val buy_df = spark.read.json(input_dir + "/buy/").filter("uid is not null")

      val actual_date = find_max_date(views_df, buy_df)

      val aggregated_views = views_df.withColumn("item_",
          expr("'view_' || lower(translate(item_id, '- ', '__'))"))
        .groupBy($"uid")
        .pivot($"item_")
        .agg(count("uid"))
        .na.fill(0)

      val aggregated_buys = buy_df.withColumn("item_",
          expr("'buy_' || lower(translate(item_id, '- ', '__'))"))
        .groupBy($"uid")
        .pivot($"item_")
        .agg(count("uid"))
        .na.fill(0)

      val join_condition = col("vw.uid") === col("buy.uid")
      val commonDF = aggregated_views.alias("vw")
        .join(aggregated_buys.alias("buy"), join_condition, "full")
        .select(expr("coalesce(vw.uid, buy.uid)").alias("total_uid"), col("*"))
        .drop($"vw.uid")
        .drop($"buy.uid")
        .na.fill(0)
        .withColumnRenamed("total_uid", "uid")

      commonDF.write.mode("overwrite").parquet(output_dir + "/" + actual_date + "/")
    }
    else if (update == 1) {

      val last_version = returnLastVersion(output_dir)
      val previousMatrix = spark.read.parquet(s"${output_dir}/${last_version}")

      val newDataView = spark.read.json(s"${input_dir}/view/").filter('p_date > last_version)
      val newDataBuy = spark.read.json(s"${input_dir}/buy/").filter('p_date > last_version)

      val new_max_date = find_max_date(newDataView, newDataBuy)

      val columnsArray = previousMatrix.columns.drop(1)
      val colsLst = (
        for (i <- 0 to columnsArray.length - 1)
          yield {
            if (i == columnsArray.length - 1) {
              s"${columnsArray(i)}, '${columnsArray(i)}'"
            }
            else {
              s"${columnsArray(i)}, '${columnsArray(i)}',"
            }
          }
        ).toArray
        .mkString("")

      val unpivotedPrevDF = previousMatrix.select($"uid", expr(s"stack(${columnsArray.length}, ${colsLst}) as (counts, item_)"))

      val transformedViewDF = newDataView.filter("uid is not null")
        .withColumn("item_",
          expr("'view_' || lower(translate(item_id, '- ', '__'))"))
        .groupBy($"uid", $"item_")
        .agg(count("uid").alias("counts"))
        .na.fill(0)
        .select($"uid", $"counts", $"item_")

      val transformedBuyDF = newDataBuy.filter("uid is not null")
        .withColumn("item_",
          expr("'buy_' || lower(translate(item_id, '- ', '__'))"))
        .groupBy($"uid", $"item_")
        .agg(count("uid").alias("counts"))
        .na.fill(0)
        .select($"uid", $"counts", $"item_")

      unpivotedPrevDF.union(transformedViewDF)
        .union(transformedBuyDF)
        .groupBy($"uid")
        .pivot("item_")
        .agg(sum($"counts"))
        .na.fill(0)
        .write
        .mode("overwrite")
        .parquet(s"${output_dir}/${new_max_date}")

      }
    }

}
