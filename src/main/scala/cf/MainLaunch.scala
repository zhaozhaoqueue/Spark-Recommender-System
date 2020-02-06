package cf

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MainLaunch {
  def main(args: Array[String]) = {
    if(args.length < 3){
      System.err.println("Identify the class to launch, the data table to load, the table to write data")
      System.exit(2)
    }
    val class_to_launch = args(0)
    val table_load = args(1)
    val table_write = args(2)
//    val other_args = args.slice(2, args.length)

    val spark = SparkSession
      .builder
      .appName(class_to_launch)
      .enableHiveSupport()
      .getOrCreate()

    // load data from hive
    import spark.sql
    // use the database ml_100
    sql("use ml_100")
    val df = sql("select * from " + table_load)

    // launch the corresponding class
    if(class_to_launch == "user"){
      val df_sim_sorted = UserBasedCF.getUserSimilarities(df, 10)
      val df_recommend = UserBasedCF.getRecommendItemList(df_sim_sorted, df, 10)
      // write to hive table
      writeToHive(df_recommend, table_write)
    }else if(class_to_launch == "item"){
      val df_sim_list = ItemBasedCF.getItemSimilaritiesADC(df)
      val df_recommend = ItemBasedCF.getRatePredictionList(df, df_sim_list)
      // write to hive table
      writeToHive(df_recommend, table_write)
    }
  }

  def writeToHive(df_recommend: DataFrame, table_name: String) = {
    import df_recommend.sparkSession.sql
    sql("create table " + table_name + "(uid int, recommend_list string) stored as parquet")
    df_recommend.write.mode(SaveMode.Overwrite).saveAsTable(table_name)
  }
}
