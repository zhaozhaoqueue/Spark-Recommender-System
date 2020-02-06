package user

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object UserBasedCF {
  def getUserSimilarities(df: DataFrame, topN: Int): DataFrame = {
    import df.sparkSession.implicits._
    val doubleFormatUDF = udf{(num: Double) => f"$num%.2f".toDouble}
    df.sparkSession.udf.register("doubleFormatUDF", doubleFormatUDF)

    val df_rate_sqr = df.selectExpr("uid", "mid", "pow(rate, 2) as rate_square")
      .groupBy("uid")
      .agg("rate_square" -> "sum")
      .withColumnRenamed("sum(rate_square)", "sum_rate_square")
      .selectExpr("uid", "sqrt(sum_rate_square) as sqrt_rate")
    val df_rate_sqr_cp = df_rate_sqr.selectExpr("uid as uid1", "sqrt_rate as sqrt_rate1")
    val df_pair_user_rate_sqr = df_rate_sqr.crossJoin(df_rate_sqr_cp)
      .filter("uid != uid1")
      .selectExpr("uid", "uid1", "sqrt_rate*sqrt_rate1 as sqrt_prod")

    val df_cp = df.selectExpr("uid as uid1", "mid", "rate as rate1")
    val df_rate_prod = df.join(df_cp, "mid")
      .filter("uid != uid1")
      .selectExpr("uid", "uid1", "mid", "rate*rate1 as rate_prod")
      .groupBy("uid", "uid1")
      .sum("rate_prod")
      .withColumnRenamed("sum(rate_prod)", "sum_rate_prod")
//      .selectExpr("uid", "uid1", "sum_rate_prod")

    val df_sim_coeff = df_rate_prod.join(df_pair_user_rate_sqr, Seq("uid", "uid1"), "inner")
      .selectExpr("uid", "uid1", "doubleFormatUDF(sum_rate_prod*1.0/sqrt_prod) as sim_coeff")

    val df_sim_sorted = df_sim_coeff
      .rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey
      .flatMapValues{x =>
        x.toArray
          .sortWith(_._2.toDouble>_._2.toDouble)
          .slice(0, if(x.toArray.length < topN) x.toArray.length else topN)
      }.toDF("uid", "sim_coeff")
//      .toDF("uid", "sorted_sim_list")

    df_sim_sorted
  }

  def getRecommendItemList(df_sim_sorted: DataFrame, df: DataFrame, topN: Int): DataFrame={
    import df.sparkSession.implicits._
    val simRateProdUDF = udf{(sim: String, rate_list: String) =>
      rate_list.split(",")
        .map{x =>
          val untruncated = (x.split("_")(1).toDouble*sim.toDouble)
          x.split("_")(0) + "_" + f"$untruncated%.2f"
        }
        .mkString(",")
    }
    val filterItemUDF = udf{(sim_prod_itemlist: String, itemlist: String) =>
      val filterMap = itemlist.split(",")
        .map(x => (x.split("_")(0), x.split("_")(1))).toMap
      sim_prod_itemlist.split(",")
        .filter(x => filterMap.getOrElse(x.split("_")(0), -1) == -1)
        .slice(0, topN)
        .mkString(",")
    }

    val df_user2itemlist = df.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey
      .mapValues{x =>
        x.toArray
          .map(x => x._1 + "_" + x._2)
          .mkString(",")
      }.toDF("uid", "itemlist")

    val df_sim_top = df_sim_sorted.selectExpr("uid", "sim_coeff._1 as sim_uid", "sim_coeff._2 as coeff")

    val df_recommend = df_sim_top.join(df_user2itemlist, df_sim_top("sim_uid") === df_user2itemlist("uid"))
      .drop(df_user2itemlist("uid"))
      .withColumn("sim_prod_itemlist", simRateProdUDF(col("coeff"), col("itemlist")))
      .select("uid", "sim_prod_itemlist")
      .rdd.map(x => (x(0).toString, x(1).toString))
      .reduceByKey((a: String, b: String) => a + ", " + b)
      .mapValues{x =>
        x.split(",")
          .map(x => (x.split("_")(0), x.split("_")(1).toDouble))
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum).toArray
          .sortWith(_._2 > _._2)
          .map(x => x._1 + "_" + x._2.toString)
          .mkString(",")
      }.toDF("uid", "sim_prod_itemlist")
      .join(df_user2itemlist, "uid")
      .drop(df_user2itemlist("uid"))
      .withColumn("recommend_list", filterItemUDF(col("sim_prod_itemlist"), col("itemlist")))
      .selectExpr("cast(uid as int)", "recommend_list")

    df_recommend
  }

  def writeToHive(df_recommend: DataFrame, table_name: String) = {
    import df_recommend.sparkSession.sql
    sql("create table " + table_name + "(uid int, recommend_list string) stored as parquet")
    df_recommend.write.mode(SaveMode.Overwrite).saveAsTable(table_name)
  }
}
