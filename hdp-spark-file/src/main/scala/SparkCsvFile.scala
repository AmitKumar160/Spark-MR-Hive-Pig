/**
  * Created by Amit k on 13-01-2019.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SparkCsvFile {
  def main(args: Array[String]): Unit = {
        val spark=SparkSession.builder().master("local[*]").appName("SparkCsvFile").getOrCreate()
        val sc = spark.sparkContext
        val sqlcontext= spark.sqlContext
        //if running on local disable args and enable location
        var inpath:String=args(0).toString    //"file:///C:/Users/Public/file.csv"
        var outpath:String=args(1).toString  //"file:///C:/Users/Public/out"
    val schema= new StructType().add("userID",StringType).add("productID",StringType).add("action",StringType)
    val shopping_df=sqlcontext.read.format("csv").option("header","true").schema(schema).load(inpath)
    //shopping_df.show()
    shopping_df.createOrReplaceTempView("shopping_details")
    val shopping_add=sqlcontext.sql("select userID,productID from shopping_details where action='AddToCart'")
    //shopping_add.show()
    val shopping_pur=sqlcontext.sql("select userID,productID from shopping_details where action='Purchase'")
    //shopping_pur.show()
    val finalDf=shopping_add.select(shopping_add("userID"),shopping_add("productID")).except(shopping_pur)
    //val joinDf=shopping_add.join(shopping_pur,shopping_add("userID")===shopping_pur("userID") && shopping_add("productID")===shopping_pur("productID"),"leftouter")
      //.where(shopping_pur("userID")===null && shopping_pur("productID")===null)
    print("************Final Output**********")
    finalDf.show()
    //joinDf.show()
    finalDf.repartition(1).write.option("header","true").csv(outpath)
    print("**********************************")
    spark.close()
  }
}
