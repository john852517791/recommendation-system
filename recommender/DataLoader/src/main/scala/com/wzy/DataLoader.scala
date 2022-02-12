package com.wzy

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
*@Auther: wzy
*@Date:2021/5/9 -05-09
*@Descreption: com.wzy
*@Version: 1.0
*/


/**product数据集
 * 6797                                                                              商品id
 * PHILIPS飞利浦HQ912/15两刀头充电式电动剃须刀                                          商品名称
 * 222,621,691                                                                        商品分类id【此处不需要】
 * B002TKLK0S                                                                         亚马逊id【此处不需要】
 * https://images-cn-4.ssl-images-amazon.com/images/I/415UjOLnBML._SY300_QL70_.jpg    商品图片
 * 家用电器|个人护理电器|电动剃须刀                                                      商品分类
 * 飞利浦|剃须刀|家用电器|好用|外观漂亮                                                   商品标签
 * */
//根据上面的来创建实体类
case class Product(
                    productId: Int,
                    name: String,
                    imageUrl: String,
                    categories: String,
                    tags: String
                  )

/**rating数据集
 * 42103,       用户id
 * 457976,      商品id
 * 4.0,         评分
 * 1215878400   打分的时间戳
 * */
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



object DataLoader {

  //    定义文件路径
  val PRODUCT_DATA_PATH="D:\\java\\workplace\\WzyRecommendationSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH="D:\\java\\workplace\\WzyRecommendationSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  //    定义mongodb中存储的表的名称
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"


  def main(args: Array[String]): Unit = {
    //    映射参数
    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/wzyrecommender",
      "mongo.db" -> "wzyrecommender"
    )

    //    创建sparkconf配置
    val sparkConf = new SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    //导入隐饰操作，否则下面的RDD无法调用toDF方法
    import spark.implicits._


    val productRDD=spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    //    将数据存入mongodb需要将rdd改为frame文件
    val productDF= productRDD.map(item=>{
      //    先将数据进行切分，以^为分隔符，\\为转义
      val attr=item.split("\\^")
      //      将切分后的数据转换成product
      Product(
        attr(0).toInt,
        //attr的数字对应上面的数据，从0开始
        attr(1).trim,
        attr(4).trim,
        attr(5).trim,
        attr(6).trim
      )
    }
    ).toDF()

    //同上,切分使用逗号
    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(
          attr(0).toInt,
          attr(1).toInt,
          attr(2).toDouble,
          attr(3).toInt
        )
      }
    ).toDF()


    storeDataInMongoDB( productDF ,ratingDF)

    spark.stop()
  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

    //      新建mongodb连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //如果 MongoDB 中有对应的数据库，那么应该删除
    mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    //    将当前数据存入对应表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //    对表创建索引
    mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION).createIndex(MongoDBObject("productId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("productId" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("userId" -> 1))

    mongoClient.close()


  }
}
