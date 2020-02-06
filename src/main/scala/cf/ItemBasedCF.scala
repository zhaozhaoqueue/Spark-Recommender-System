package cf

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object ItemBasedCF {
  def  getItemSimilaritiesADC(df: DataFrame): DataFrame= {
    import df.sparkSession.implicits._
    // normalize rate with mean rate of each user
    val df_user_avg = df.groupBy("uid").mean("rate").withColumnRenamed("avg(rate)", "avg_rate")
    val df_nor = df.join(df_user_avg, "uid")
      .selectExpr("uid", "mid", "rate-avg_rate as nor_rate")

    val df_sqrt = df_nor.selectExpr("uid", "mid", "pow(nor_rate,  2) as square_rate")
        .groupBy("mid")
        .agg("square_rate" -> "sum")
        .withColumnRenamed("sum(square_rate)", "sum_square_rate")
        .selectExpr("mid", "sqrt(sum_square_rate) as sqrt_sum")
//    val df_pair_sqrt_prod = df_sqrt.crossJoin(df_sqrt.selectExpr("mid as mid1", "sqrt_sum as sqrt_sum1"))
//      .filter("cast(mid as long) < cast(mid1 as long)")
//      .selectExpr("mid", "mid1", "sqrt_sum*sqrt_sum1 as sqrt_sum_prod")
//      .rdd.map(x => (x(0).toString + "_" + x(1).toString, x(2).toString))
//      .toDF("mid_pair", "sqrt_sum_prod")

    val df_nor_cp = df_nor.selectExpr("uid", "mid as mid1", "nor_rate as nor_rate1")
    val df_sum_prod = df_nor.join(df_nor_cp, "uid")
      .filter("cast(mid as long) != cast(mid1 as long)")
      .selectExpr("uid", "mid", "mid1", "nor_rate*nor_rate1 as rate_prod")
      .groupBy("mid", "mid1")
      .agg("rate_prod" -> "sum")
      .withColumnRenamed("sum(rate_prod)", "sum_rate_prod")

    val df_sim_list = df_sum_prod.join(df_sqrt, Seq("mid"), "inner")
        .join(df_sqrt.selectExpr("mid as mid1", "sqrt_sum as sqrt_sum1"), Seq("mid1"), "inner")
        .selectExpr("mid", "mid1", "sum_rate_prod/(sqrt_sum*sqrt_sum1) as sim_coeff")

//        val df_sim_list = df_sum_prod.join(df_pair_sqrt_prod, "mid_pair")
//      .selectExpr("mid_pair", "sum_rate_prod/sqrt_sum_prod as sim_coeff")
//      .rdd.map()
//      .groupByKey
//      .mapValues{x =>
//        x.toArray
//          .map(x => x._1 + "_" + x._2)
//          .mkString(",")
//      }.toDF("mid", "sim_coeff_list")

    df_sim_list
  }

  def getRatePredictionList(df: DataFrame, df_sim_list: DataFrame): DataFrame={
    import df.sparkSession.implicits._
//    val filterMap = df.rdd.collect.map(x => ((x(0).toString + "_" + x(1).toString), 1)).toMap
    val spark = df.sparkSession
    val filterRatedUDF = udf{(mid: String, ratedlist: String) =>
      val filterMap = ratedlist.split(",").map(x => (x.split("_")(0), x.split("_")(1))).toMap
      filterMap.getOrElse(mid, -1) == -1
    }
    val prodUDF = udf{(coefflist: String, ratedlist: String) =>
      val item_coef_df = spark.createDataFrame(
        coefflist.split(",")
          .map(x => (x.split("_")(0), x.split("_")(1).toDouble))
          .toSeq)
        .toDF("mid", "coeff")
      val item_rate_df = spark.createDataFrame(
        ratedlist.split(",")
          .map(x => (x.split("_")(0), x.split("_")(1).toDouble))
          .toSeq)
        .toDF("mid", "rate")
      item_rate_df.join(item_coef_df, Seq("mid"), "inner")
        .selectExpr("sum(coeff*rate) as score", "sum(coeff) as norm")
        .selectExpr("score/norm")
        .first
        .get(0).toString.toDouble
    }

    val df_user_ratedlist = convertFunction(df, "uid", "ratedlist")
    val df_item_coefflist = convertFunction(df_sim_list, "mid", "coefflist")

    val df_recommend = df_user_ratedlist.crossJoin(df_item_coefflist)
      .filter(filterRatedUDF(col("mid"), col("ratedlist")))
      .withColumn("pred_rate", prodUDF(col("coefflist"), col("ratedlist")))
      .select("uid", "mid", "pred_rate")
      .rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey
      .mapValues{x =>
        x.toArray
          .sortWith(_._2.toDouble > _._2.toDouble)
          .map(x => x._1 + "_" + x._2)
          .mkString(",")
      }.toDF("uid", "recommend_list")
      .selectExpr("cast(uid as int)", "recommend_list")

    df_recommend
  }

  def writeToHive(df_recommend: DataFrame, table_name: String): Unit ={
    import df_recommend.sparkSession.sql
    sql("create table " + table_name + "(uid int, recommend_list string) stored as parquet")
    df_recommend.write.mode(SaveMode.Overwrite).saveAsTable(table_name)
  }

  def convertFunction(df: DataFrame, col_name1: String, col_name2: String): DataFrame={
    import df.sparkSession.implicits._
    df.rdd.map(x=> (x(0).toString, (x(1).toString, x(2).toString)))
      .groupByKey
      .mapValues{x =>
        x.toArray
          .map(x => x._1 + "_" + x._2)
          .mkString(",")
      }.toDF(col_name1, col_name2)
  }
}
