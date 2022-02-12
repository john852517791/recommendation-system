package com.wzy

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/*
*@Auther: wzy
*@Date:2021/5/9 -05-09
*@Descreption: com.wzy
*@Version: 1.0
*/

//连接redis配置
object ConnHelper extends Serializable{
  lazy val jedis=new Jedis("localhost")
  //懒变量，即在使用时才会初始化
  lazy val mongoClient=MongoClient(MongoClientURI("mongodb://localhost:27017/wzyrecommender"))
}


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



object OnlineRecommender {


  //  定义常量和mongo表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS="StreamRecs"
  val PRODUCT_RECS="ProductRecs"

  val MAX_USER_RATING_NUM=20
  val MAX_SIM_PRODUCTS_NUM=20


  def main(args: Array[String]): Unit = {

    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/wzyrecommender",
      "mongo.db" -> "wzyrecommender",
      "kafka.topic"->"recommender"
    )

    //    新建spark conf
    val sparkConf = new SparkConf().setAppName("OnlineRecommender").setMaster(config("spark.cores"))
    val spark= SparkSession.builder().config(sparkConf).getOrCreate()
    val sc=spark.sparkContext




    //    让其能有2s的计算时间
    val ssc=new StreamingContext(sc,Seconds(2))

    implicit val mongoConfig=MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    //    加载数据——相似度矩阵，广播出去
    val simProductsMatrix = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd//为了使用转化为map的方法，dataframe格式无法使用collectmap方法
      .map(item=>(
        //        为了后续查询相似度更加方便一些，把数据转换为map形式（即key-value键值对）
        item.productId,item.recs.map(x=>(x.productId,x.score)).toMap
      ))
      .collectAsMap()//将整个rdd转化为map，以productid为key，键值对recs为value
    //    注意：这里有两个map，套娃为Map[Int ,Map[int, Double]]
    //
    //    定义广播变量
    val simProcutsMatrixBC = sc.broadcast(simProductsMatrix)


    //    需要使用到kafka
    //    创建kafka配置参数
    val kafkaPara = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender"
      ,"auto.offset.reset" -> "latest",
//    ,"max.poll.interval.ms" -> "500",
//    "max.poll.records" -> "50",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

////     配置kafka的偏移量
//    val offsets = collection.Map[TopicPartition, Long] {
//      new TopicPartition("recommender", 3) -> 200
//    }


    //
    //    创建一个Dstream
    val kafkaStream = KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")), kafkaPara)
      //    消费者的策略，订阅topic-recommender
    )

    //    对KafkaStream进行预处理，产生评分流，包含【userID|productID|score|timestamp】
    val ratingStream = kafkaStream.map { msg =>
      var attr = msg.value().split("\\|")
      (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }

    //    核心算法，定义流式计算的处理过程
    ratingStream.foreachRDD{
      rdds=>rdds.foreach{
        case(userId, productId, score, timestamp)=>
          println("rating data coming>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")

          //        todo:核心算法流程
          //        首先从redis里面取出当前用户的最近评分，保存成一个数组Array[(productID,score)]
          val userRecentluRatings = getUserRecentlyRatings(MAX_USER_RATING_NUM, userId,ConnHelper.jedis)

          //          随后从相似度矩阵中获取当前商品最相似的商品列表作为备选列表,保存成一个数组Array[productID]
          val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProcutsMatrixBC.value)

          //          计算每个备选商品的推荐优先级，得到当前用户的实时推荐列表，保存成Array[(productID,score)]，以score排序当作优先级
          val streamRecs =computeProductScore(candidateProducts, userRecentluRatings, simProcutsMatrixBC.value)

          //          把推荐列表保存到mongodb中
          saveDataToMongoDB(userId, streamRecs)

      }
    }

    //  启动streaming
    ssc.start()
    //    是否已经启动的标记
    println("streaming started>>>>>>>>>>>>>")
    //  等待用户的终止或者异常造成的终止
    ssc.awaitTermination()

  }

  import scala.collection.JavaConversions._
  //实现getUserRecentlyRatings函数
  //  从redis中获取最近num次的评分
  def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int,Double)]={
    //    从redis中用户的评分队列里面获取评分数据，key格式为uid:USERID，value格式为 PRODUCTID:SCORE
    jedis.lrange("userId:" + userId.toString,0,num)
      .map{ item =>
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }




  //  获取当前商品的相似列表并过滤到用户已经评分过的，最终得到备选列表

  def getTopSimProducts(
                         num: Int,
                         productId: Int,
                         userId: Int,
                         simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                       (implicit mongoConfig: MongoConfig)
  :Array[Int]={
    //    从广播变量相似度矩阵中拿到当前商品的相似度列表
    val allSimProducts = simProducts(productId).toArray

    //    获得用户已经评分过的商品
    val ratingCollection=ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ExistRating= ratingCollection.find(MongoDBObject("userId"->userId))
      .toArray
      .map{item=>
        //只需要productID
        item.get("productId").toString.toInt
      }

    allSimProducts.filter(x => ! ExistRating.contains(x._1))
      //    将已经评分过的商品给过滤掉
      .sortWith(_._2>_._2)
      //    降序
      .take(num)
      //    选取前num个
      .map(x=>x._1)
  }



  //  计算每个备选商品的预测的推荐评分
  def computeProductScore(candidateProducts: Array[Int],
                          userRecentlyRatings: Array[(Int, Double)],
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  :Array[(Int,Double)]={
    //    定义一个长度可变数组，用arraybuffer，用于保存每一个备选商品的基础得分
    val scores=scala.collection.mutable.ArrayBuffer[(Int,Double)](

    )
    //    定义两个map，用于保存每个商品的高分和低分的计数器,productID->count
    val increaseMap=scala.collection.mutable.HashMap[Int, Int]()
    val decreaseMap=scala.collection.mutable.HashMap[Int, Int]()

    //    遍历每个备选商品，计算和已被评分的商品的相似度
    for ( candidateProduct<- candidateProducts; userRecentlyRating<-userRecentlyRatings){
      //      从相似度矩阵中获取当前备选商品和当前已评分商品之间的相似度
      val simScore = getProductsSimScore(candidateProduct,userRecentlyRating._1, simProducts)
      if (simScore > 0.4){
        //        按照公式进行加权计算
        scores+=((candidateProduct,simScore*userRecentlyRating._2))
        if (userRecentlyRating._2>3){
          //          若没有值则默认设置为0
          increaseMap(candidateProduct)=increaseMap.getOrDefault(candidateProduct,0)+1
        } else {
          decreaseMap(candidateProduct)=decreaseMap.getOrDefault(candidateProduct,0)+1

        }
      }
    }

    //    根据公式计算所有的推荐优先级，首先以productID作groupby
    scores.groupBy(_._1)
      .map{
        case (productID,scoreList)=>
          (productID,scoreList.map(_._2).sum/scoreList.length + log(increaseMap.getOrDefault(productID,1)) - log(decreaseMap.getOrDefault(productID,1)))
        //第二项为平均分+加权，由于math.log中默认以e为底，所以log要自己定义
      }
      //    返回一个推荐列表，且按照得分排序
      .toArray
      .sortWith(_._2>_._2)//降序排列
  }




  //  得到相似度评分
  def getProductsSimScore(product1: Int,
                          product2: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
  :Double={
    //    做模式匹配
    simProducts.get(product1) match {
      //        若product1不为空，则sims会是一个列表，则再从列表中取出product2
      case Some(sims) => sims.get(product2) match {
        //        若能取出product2的话（即product2不为空），就取出并返回其分数
        case Some(score) =>score
        case None=>0.0
      }
      case None=>0.0
    }
  }


  //  自定义log函数
  def log(m: Int):Double={
    val N=10
    math.log(m)/math.log(N)
  }


  // 写入mongodb
  def saveDataToMongoDB(userId: Int,
                        streamRecs: Array[(Int, Double)])
                       (implicit mongoConfig: MongoConfig): Unit ={
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove( MongoDBObject( "userId" -> userId ) )
    streamRecsCollection.insert( MongoDBObject( "userId" -> userId,
      "recs" -> streamRecs.map(x =>MongoDBObject("productId"->x._1, "score"->x._2)) ) )
  }

}
