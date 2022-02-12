package com.wzy

import breeze.numerics.sqrt
import com.wzy.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/*
*@Auther: wzy
*@Date:2021/5/9 -05-09
*@Descreption: com.wzy
*@Version: 1.0
*/



object ALSTrainer {

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
        rating =>Rating (rating.userId, rating.productId, rating.score)
      ).cache()//持久化到内存中,优化步骤


    //    将数据集切分为训练集与测试集，占比为4：1
    //    注意：由于数据集划分是随机的，因此这个单例每次跑出来的结果都可能不同
    val splits=ratingRDD.randomSplit(Array(0.8,0.2))
    val trainingRDD=splits(0)
    val testingRDD= splits(1)

    //    核心实现：输出最优参数到控制台
    adjustALSParams(trainingRDD,testingRDD)

    spark.stop()
  }


  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]):Unit={
    //  for循环依次尝试这些参数，其中迭代次数当然相对越大越好，但没必要多次尝试，这里直接给定为10
    val result=for(rank<- Array(5,10,20,50);lambda<-Array(1,0.1,0.01))yield {
      val model=ALS.train(trainData,rank,10,lambda)
      val rmse=getRMSE(model,testData)
      (rank,lambda,rmse)
    }
    //    按照rmse均方根误差排序，并输出最优的一个，即最小的rmse
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double={
    //    先构建userProducts，得到预测评分矩阵
    val userProducts=data.map(item=>(item.user,item.product))
    val predictRating=model.predict(userProducts)

    //    按照公式计算rmse
    //    先把真实评分与预测评分按照（userid，productid）做连接
    //  真实评分
    val observed=data.map(item=>((item.user,item.product),item.rating))
    //  预测评分
    val predict=data.map(item=>((item.user,item.product),item.rating))

    sqrt(//开根号
      observed.join(predict).map{
        case ((userID,productID),(actual,pre))=>
          val err=actual-pre
          err*err
      }.mean()
    )
  }

}
