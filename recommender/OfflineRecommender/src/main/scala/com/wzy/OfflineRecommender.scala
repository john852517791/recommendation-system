package com.wzy

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix


/*
*@Auther: wzy
*@Date:2021/5/9 -05-09
*@Descreption: com.wzy
*@Version: 1.0
*/


case class ProductRating(
                          userId: Int,
                          productId: Int,
                          score: Double
//                          , timeStamp: Int
                        )
case class MongoConfig(
                        uri: String,
                        db: String
                      )
//标准推荐对象
case class Recommendation(productId:Int, score: Double)
//用户的推荐列表
case class UserRecs(userId: Int,recs: Seq[Recommendation])
//定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])


object OfflineRecommender {

  //  使用rating的collection
  val MONGODB_RATING_COLLECTION = "Rating"
  //  定义一个用户推荐列表
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  //  定义最大推荐数为20
  val USER_MAX_RECOMMENDATION = 20



  def main(args: Array[String]): Unit = {

    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/wzyrecommender",
      "mongo.db" -> "wzyrecommender"
    )
    //  创建配置
    val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    //导入隐饰操作，否则下面的RDD无法调用toDF方法
    import spark.implicits._

    //    加载数据,转化为RDD
    // 加载数据

    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => (rating.userId, rating.productId, rating.score)
      ).cache()//持久化到内存中,优化步骤

    //    提取出所有用户和商品的数据集,distinct用于去重复
    val userRDD=ratingRDD.map(_._1).distinct()
    val productRDD=ratingRDD.map(_._2).distinct()

    //    TODO:核心计算步骤
    //    1.训练隐语义模型
    //    首先得到traindata
    val trainData = ratingRDD.map(x=> Rating(x._1, x._2, x._3))
    //    隐特征的维度(K个),迭代次数,正则化系数
    //    先拍脑袋直接写参数
    val (rank, iterations, lambda)=(5,10,0.01)
    val model= ALS.train(trainData, rank, iterations, lambda)
    //上面这个model就是得到的模型
    //    2.获得预测评分矩阵,得到用户的推荐列表
    //    用userRDD和productRDD作笛卡尔积得到一个空的userProductRDD
    val userProducts=userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)
    //    从预测评分矩阵中提取得到用户推荐列表
    //    其中对预测评分作预筛选，比如下面的筛选是要求评分大于0
    val userRecs=preRating.filter(_.rating>0)
      .map(
        rating=>(rating.user,(rating.product,rating.rating))
      )
      .groupByKey()
      .map{
        case (userID,recs)=>
          //            _._2>_._2表示筛选中用降序
          UserRecs(userID,recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)))
      }.toDF()//为了写入mongodb还是要将rdd转为dataframe

    //    写入mongodb
    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    //    3.利用商品的特征向量,计算商品的相似度列表
    //    先拿到商品的特征向量
    val productFeatures= model.productFeatures.map{
      case(productID,features)=>(productID,new DoubleMatrix(features))
      //    doublematrix可以通过传入一个数组将其转换为一个矩阵，以便于向量计算
    }
    //    再两两配对商品来计算余弦相似度, 这里就让商品特征向量与自身作笛卡尔积
    val productRecs=productFeatures.cartesian(productFeatures)
      .filter{
        case (a,b)=>a._1 != b._1//即productid不能相同
      }
      //由于与自身作笛卡尔积，每次都会与自己作相似度计算，那么自身排序就会靠前，因此需要过滤掉与自身作相似度计算
      //        下面就是计算余弦相似度
      .map{
        case(a,b)=>
          //          cosSim函数计算余弦相似度
          val simScore=cosSim(a._2,b._2)
          (a._1,(b._1,simScore))
      }
      //      拍脑袋过滤余弦值小于0.4的商品
      .filter(_._2._2>0.4)
      .groupByKey()
      .map{
        case (productID, recs)=>
          ProductRecs(productID,recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)))
      }.toDF()

    productRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()





    spark.stop()
  }

  def cosSim(product1: DoubleMatrix, product2: DoubleMatrix):Double={
    //   dot即点乘，模长就是L-2范数（即norm2）
    product1.dot(product2)/(product1.norm2()*product2.norm2())
  }


}
