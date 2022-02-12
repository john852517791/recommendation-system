import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
*@Auther: wzy
*@Date:2021/5/12 -05-12
*@Descreption: 
*@Version: 1.0
*/


//todo：根据相似用户来过滤商品，这一部分不需要mllib
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
//定义商品相似度列表
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object ItemCFRecommender {

//  定义常量，指定表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10


  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/wzyrecommender",
      "mongo.db" -> "wzyrecommender"
    )
    //  创建配置
    val sparkConf = new SparkConf().setAppName("ItemCFRecommender").setMaster(config("spark.cores"))
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    //导入隐饰操作，否则下面的RDD无法调用toDF方法
    import spark.implicits._


//    加载数据，转换成DF格式
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(
        //        由于不需要时间戳，这里可以把时间戳略去
        x => ( x.userId, x.productId, x.score )
      )
      .toDF("userId", "productId", "score")
      .cache()

//    todo:核心算法，计算同现相似度，得到商品的相似列表

//    todo:先统计每个商品的评分个数
//    按productId做group by
    val productRatingCountDF = ratingDF.groupBy("productId").count()
//    productRatingCountDF.show()

//    再原有的评分表上rating添加count，使用默认内连接
    val ratingWithCountDF = ratingDF.join(productRatingCountDF,"productId")

//    将评分按照用户id来两两配对，统计两个商品被同一个用户评分过的次数
//    即得出一个表，每行都是一个用户对两个商品的评分
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2")
      .select("userId","product1","count1","product2","count2")
//    joinedDF.show()
//    用joinedDF创建一张临时表用于下一步写sql查询
    joinedDF.createOrReplaceTempView("joined")

    val CooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1,product2
        |""".stripMargin
    ).cache()

//    提取所需要的数据， 将其包装成(productId1,(product2,score))
    val simDF = CooccurrenceDF.map {
      row =>
        val coocSim = CooccurrenceSim(
          row.getAs[Long]("cocount"),
          row.getAs[Long]("count1"),
          row.getAs[Long]("count2")
        )
        (row.getInt(0),(row.getInt(1),coocSim))
    }
      .rdd
      .groupByKey()
      .map{
        case (productId, recs)=>
          ProductRecs(
            productId,
            recs.toList
                .filter(x=>x._1 != productId)
                .sortWith(_._2>_._2)
                .take(MAX_RECOMMENDATION)
                .map(x=>Recommendation(x._1,x._2)))
      }.toDF()





    simDF.write
      .option("uri",mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()

  }

//  按照公式计算同现相似度
  def CooccurrenceSim(coCount: Long, count1: Long, count2: Long)
  :Double={
    coCount/math.sqrt(count1*count2)
  }
}
