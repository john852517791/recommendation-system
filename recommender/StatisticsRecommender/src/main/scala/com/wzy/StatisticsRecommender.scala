package com.wzy

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
*@Auther: wzy
*@Date:2021/5/9 -05-09
*@Descreption: com.wzy
*@Version: 1.0
*/

case class Rating(
                   userId: Int,
                   productId: Int,
                   score: Double,
                   timeStamp: Int
                 )


/**
 * mongodb连接配置
 * mongodb的连接url
 * 需要操作的数据库db
 * */
case class MongoConfig(
                        uri:String,
                        db: String
                      )


object StatisticsRecommender {

  //  使用rating的collection
  val MONGODB_RATING_COLLECTION = "Rating"
  //  历史热门统计表
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  //  最近热门统计表
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  //  每个商品的平均评分统计表
  val AVERAGE_PRODUCTS = "AverageProductsScore"

  def main(args: Array[String]): Unit = {

    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/wzyrecommender",
      "mongo.db" -> "wzyrecommender"
    )
    //  创建配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    //导入隐饰操作，否则下面的RDD无法调用toDF方法
    import spark.implicits._

    //    加载数据,即rating表中的评分数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection",MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    //    创建一张叫ratings的临时表[相当于mysql中的临时视图]
    ratingDF.createOrReplaceTempView("ratings")
    //    目的----用sparksql去做不同的统计推荐




    //    1.历史热门商品,按照评分的个数统计
    //    思路:统计历史评分数量,按大小排序
    val rateMoreProductDF= spark.sql(
      "select productId, count(productId) as count from ratings group by productId order by count desc"
    )
    storeDFInMongoDB(rateMoreProductDF,RATE_MORE_PRODUCTS)




    //    2.近期热门商品,把时间戳转化为yyyyMM格式来进行评分个数统计
    //    最终目的是得到productID,count以及yearmonth
    //    首先搞一个日期格式化工具
    val SimpleDateFormat = new SimpleDateFormat("yyyyMM")
    //    注册UDF,将时间戳转化为yyyyMM格式
    //注意,这里javautil的date时间单位为毫秒,所以应该×1000进为秒,同时经过乘法进行强制类型转换.
    // 最后由于format转化的是string类型,因此还要再改为int类型方便排序
    spark.udf.register("changeDate",(x: Int)=>SimpleDateFormat.format(new Date(x * 1000L)).toInt)
    //    把原始rating数据转化为想要的结构productId,score,yearmouth
    val ratingOfYearMonthDF = spark.sql(
      "select productId, score, changeDate(timeStamp) as yearmonth from ratings"
    )
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsFD=spark.sql(
      "select productId, count(productId) as count ,yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth, count desc "
    )
    //    把df保存到mongodb,调用之前的函数
    storeDFInMongoDB(rateMoreRecentlyProductsFD,RATE_MORE_RECENTLY_PRODUCTS)




    //    3.优质商品统计,即平均评分高的商品
    val averageProductsDF=spark.sql(
      "select productId, avg(score) as avg from ratings group by productId order by avg desc"
    )
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)


    spark.stop()
  }


  //  历史热门的数据库录入实现
  def storeDFInMongoDB(df: DataFrame, collection_name: String)
                      (implicit mongoConfig: MongoConfig):Unit={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }



}
