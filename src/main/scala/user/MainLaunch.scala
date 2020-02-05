package user

import org.apache.spark.sql.SparkSession

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
      UserBasedCF.writeToHive(df_recommend, table_write)
    }


  }
}
