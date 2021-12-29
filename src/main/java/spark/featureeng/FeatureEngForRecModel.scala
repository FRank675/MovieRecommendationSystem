package spark.featureeng

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{format_number, _}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

import scala.collection.immutable.ListMap
import scala.collection.{JavaConversions, mutable}

//1、为ratings数据添加标签。即对每条评分数据，把评分大于等于 3.5 分的样本标签标识为 1，意为“喜欢”，评分小于 3.5 分的样本标签标识为 0，意为“不喜欢”。
//2、制作电影特征和用户特征
//3、合并特征后存储到本地以及redis
object FeatureEngForRecModel {

  val NUMBER_PRECISION = 2
  //redis端口位置
  val redisEndpoint = "localhost"
  val redisPort = 6379

  //给rating文件添加标签
  def addSampleLabel(ratingSamples:DataFrame): DataFrame ={
    //展示ratingSample前十行
    ratingSamples.show(10, truncate = false)
    //输出ratingSample的schema
    ratingSamples.printSchema()

    //注意下面的操作并不会改变ratingSamples本身，会得到一个新的dataframe
    //先按rating分组，然后统计每个rating的个数然后排序，然后都除以rating总数得到百分比。
    val sampleCount = ratingSamples.count()
    ratingSamples.groupBy(col("rating")).count().orderBy(col("rating"))
      .withColumn("percentage", col("count")/sampleCount).show(100,truncate = false)

    //归一化处理
    ratingSamples.withColumn("label", when(col("rating") >= 3.5, 1).otherwise(0))
  }

  //加电影画像
  def addMovieFeatures(movieSamples:DataFrame, ratingSamples:DataFrame): DataFrame ={

    //往评分表中融入电影信息。left表示以评分表为基准表,就是以ratingSamples中的movieId
    //作为索引去movieSamples里去查，有就拿过来拼上，没有就为null
    //还有inner和right,inner是两个表在指定列下的共同部分的融合。right以后者为基准表
    val samplesWithMovies1 = ratingSamples.join(movieSamples, Seq("movieId"), "left")
    //提取出电影年份，电影出版时间
    val extractReleaseYearUdf = udf({(title: String) => {
      if (null == title || title.trim.length < 6) {
        1990.toString // default value
      }
      else {
        val yearString = title.trim.substring(title.length - 5, title.length - 1)
        yearString
      }
    }})

    //添加电影名称，除去空格后删掉后面的年份
    val extractTitleUdf = udf({(title: String) => {title.trim.substring(0, title.trim.length - 6).trim}})

    val samplesWithMovies2 = samplesWithMovies1.withColumn("releaseYear", extractReleaseYearUdf(col("title")))
      .withColumn("title", extractTitleUdf(col("title")))
      .drop("title")  //title is useless currently

    //提取电影类别
    val samplesWithMovies3 = samplesWithMovies2.withColumn("movieGenre1",split(col("genres"),"\\|").getItem(0))
      .withColumn("movieGenre2",split(col("genres"),"\\|").getItem(1))
      .withColumn("movieGenre3",split(col("genres"),"\\|").getItem(2))

    //增加电影评分数、平均分、标准差
    //利用 Spark 中的 groupBy 操作，将原始评分数据按照 movieId 分组，然后用 agg 聚合操作来计算一些统计型特征。
    // 比如，在上面的代码中，我们就分别使用了 count 内置聚合函数来统计电影评价次数（movieRatingCount），用 avg 函数来统计评分均值（movieAvgRating），
    // 以及使用 stddev 函数来计算评价分数的标准差（movieRatingStddev）
    val movieRatingFeatures = samplesWithMovies3.groupBy(col("movieId"))
      .agg(count(lit(1)).as("movieRatingCount"),
        format_number(avg(col("rating")), NUMBER_PRECISION).as("movieAvgRating"),
        stddev(col("rating")).as("movieRatingStddev"))
    .na.fill(0).withColumn("movieRatingStddev",format_number(col("movieRatingStddev"), NUMBER_PRECISION))


    //将上面得到的特征和之前的dataframe拼接起来，因为groupby后得到的是一个新的dataframe
    val samplesWithMovies4 = samplesWithMovies3.join(movieRatingFeatures, Seq("movieId"), "left")
    samplesWithMovies4.printSchema()
    samplesWithMovies4.show(10, truncate = false)

    samplesWithMovies4
  }

  //电影风格
  val extractGenres: UserDefinedFunction = udf { (genreArray: Seq[String]) => {
    val genreMap = mutable.Map[String, Int]()
    genreArray.foreach((element:String) => {
      val genres = element.split("\\|")
      genres.foreach((oneGenre:String) => {
        genreMap(oneGenre) = genreMap.getOrElse[Int](oneGenre, 0)  + 1
      })
    })
    val sortedGenres = ListMap(genreMap.toSeq.sortWith(_._2 > _._2):_*)
    sortedGenres.keys.toSeq
  }}

  //添加用户特征
  def addUserFeatures(ratingSamples:DataFrame): DataFrame ={
    //先按照userId进行分组，然后在每个userId下按照时间进行排序
    val samplesWithUserFeatures = ratingSamples
      .withColumn("userPositiveHistory", collect_list(when(col("label") === 1, col("movieId")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      //对用户的计数操作、取平均操作等等。
      .withColumn("userPositiveHistory", reverse(col("userPositiveHistory")))
      //用户好评电影ID
      .withColumn("userRatedMovie1",col("userPositiveHistory").getItem(0))
      .withColumn("userRatedMovie2",col("userPositiveHistory").getItem(1))
      .withColumn("userRatedMovie3",col("userPositiveHistory").getItem(2))
      .withColumn("userRatedMovie4",col("userPositiveHistory").getItem(3))
      .withColumn("userRatedMovie5",col("userPositiveHistory").getItem(4))
      //用户评分总分
      .withColumn("userRatingCount", count(lit(1))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      //用户好评电影的发布年均均值
      .withColumn("userAvgReleaseYear", avg(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)).cast(IntegerType))
      //用户好评电影的发布年份标准差
      .withColumn("userReleaseYearStddev", stddev(col("releaseYear"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      //用户评分总分
      .withColumn("userAvgRating", format_number(avg(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)), NUMBER_PRECISION))
      //用户评分标准差
      .withColumn("userRatingStddev", stddev(col("rating"))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1)))
      //用户最喜欢的电影风格
      .withColumn("userGenres", extractGenres(collect_list(when(col("label") === 1, col("genres")).otherwise(lit(null)))
        .over(Window.partitionBy("userId")
          .orderBy(col("timestamp")).rowsBetween(-100, -1))))
      .na.fill(0)
      .withColumn("userRatingStddev",format_number(col("userRatingStddev"), NUMBER_PRECISION))
      .withColumn("userReleaseYearStddev",format_number(col("userReleaseYearStddev"), NUMBER_PRECISION))
      .withColumn("userGenre1",col("userGenres").getItem(0))
      .withColumn("userGenre2",col("userGenres").getItem(1))
      .withColumn("userGenre3",col("userGenres").getItem(2))
      .withColumn("userGenre4",col("userGenres").getItem(3))
      .withColumn("userGenre5",col("userGenres").getItem(4))
      .drop("genres", "userGenres", "userPositiveHistory")
      .filter(col("userRatingCount") > 1)

    samplesWithUserFeatures.printSchema()
    samplesWithUserFeatures.show(100, truncate = false)

    samplesWithUserFeatures
  }

  //存储电影特征到redis
  def extractAndSaveMovieFeaturesToRedis(samples:DataFrame): DataFrame = {
    val movieLatestSamples = samples.withColumn("movieRowNum", row_number()
      .over(Window.partitionBy("movieId")
        .orderBy(col("timestamp").desc)))
      .filter(col("movieRowNum") === 1)
      .select("movieId","releaseYear", "movieGenre1","movieGenre2","movieGenre3","movieRatingCount",
        "movieAvgRating", "movieRatingStddev")
      .na.fill("")

    movieLatestSamples.printSchema()
    movieLatestSamples.show(100, truncate = false)

    val movieFeaturePrefix = "mf:"

    val redisClient = new Jedis(redisEndpoint, redisPort)
//    val params = SetParams.setParams()
//    //set ttl to 24hs * 30
//    params.ex(60 * 60 * 24 * 30)
    val sampleArray = movieLatestSamples.collect()
    println("total movie size:" + sampleArray.length)
    var insertedMovieNumber = 0
    val movieCount = sampleArray.length
    for (sample <- sampleArray){
      val movieKey = movieFeaturePrefix + sample.getAs[String]("movieId")
      val valueMap = mutable.Map[String, String]()
      valueMap("movieGenre1") = sample.getAs[String]("movieGenre1")
      valueMap("movieGenre2") = sample.getAs[String]("movieGenre2")
      valueMap("movieGenre3") = sample.getAs[String]("movieGenre3")
      valueMap("movieRatingCount") = sample.getAs[Long]("movieRatingCount").toString
      valueMap("releaseYear") = sample.getAs[Int]("releaseYear").toString
      valueMap("movieAvgRating") = sample.getAs[String]("movieAvgRating")
      valueMap("movieRatingStddev") = sample.getAs[String]("movieRatingStddev")

      redisClient.hset(movieKey.toString, JavaConversions.mapAsJavaMap(valueMap).toString , "")
      insertedMovieNumber += 1
      if (insertedMovieNumber % 100 ==0){
        println(insertedMovieNumber + "/" + movieCount + "...")
      }
    }

    redisClient.close()
    movieLatestSamples
  }

  //划分训练和测试模型
  def splitAndSaveTrainingTestSamples(samples:DataFrame, savePath:String)={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1)

    println("!!!!!!!!!!!!begin splitAndSaveTrainingTestSamples")
    //split training and test set by 8:2
    val Array(training, test) = smallSamples.randomSplit(Array(0.8, 0.2))

    val sampleResourcesPath = this.getClass.getResource(savePath)

    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/testSamples")

  }

  def splitAndSaveTrainingTestSamplesByTimeStamp(samples:DataFrame, savePath:String)={
    //generate a smaller sample set for demo
    val smallSamples = samples.sample(0.1).withColumn("timestampLong", col("timestamp").cast(LongType))

    val quantile = smallSamples.stat.approxQuantile("timestampLong", Array(0.8), 0.05)
    val splitTimestamp = quantile.apply(0)

    val training = smallSamples.where(col("timestampLong") <= splitTimestamp).drop("timestampLong")
    val test = smallSamples.where(col("timestampLong") > splitTimestamp).drop("timestampLong")

    val sampleResourcesPath = this.getClass.getResource(savePath)
    training.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/trainingSamples")
    test.repartition(1).write.option("header", "true").mode(SaveMode.Overwrite)
      .csv(sampleResourcesPath+"/testSamples")
  }

  //划分训练和测试模型
  def extractAndSaveUserFeaturesToRedis(samples:DataFrame): DataFrame = {
    val userLatestSamples = samples.withColumn("userRowNum", row_number()
      .over(Window.partitionBy("userId")
        .orderBy(col("timestamp").desc)))
      .filter(col("userRowNum") === 1)
      //user特征
      .select("userId","userRatedMovie1", "userRatedMovie2","userRatedMovie3","userRatedMovie4","userRatedMovie5",
        "userRatingCount", "userAvgReleaseYear", "userReleaseYearStddev", "userAvgRating", "userRatingStddev",
        "userGenre1", "userGenre2","userGenre3","userGenre4","userGenre5")
      .na.fill("")

    userLatestSamples.printSchema()
    userLatestSamples.show(100, truncate = false)

    val userFeaturePrefix = "uf:"

    val redisClient = new Jedis(redisEndpoint, redisPort)
//    val params = SetParams.setParams()
//    //set ttl to 24hs * 30
//    params.ex(60 * 60 * 24 * 30)
    val sampleArray = userLatestSamples.collect()
    println("total user size:" + sampleArray.length)
    var insertedUserNumber = 0
    val userCount = sampleArray.length
    for (sample <- sampleArray){
      val userKey = userFeaturePrefix + sample.getAs[String]("userId")
      val valueMap = mutable.Map[String, String]()
      valueMap("userRatedMovie1") = sample.getAs[String]("userRatedMovie1")
      valueMap("userRatedMovie2") = sample.getAs[String]("userRatedMovie2")
      valueMap("userRatedMovie3") = sample.getAs[String]("userRatedMovie3")
      valueMap("userRatedMovie4") = sample.getAs[String]("userRatedMovie4")
      valueMap("userRatedMovie5") = sample.getAs[String]("userRatedMovie5")
      valueMap("userGenre1") = sample.getAs[String]("userGenre1")
      valueMap("userGenre2") = sample.getAs[String]("userGenre2")
      valueMap("userGenre3") = sample.getAs[String]("userGenre3")
      valueMap("userGenre4") = sample.getAs[String]("userGenre4")
      valueMap("userGenre5") = sample.getAs[String]("userGenre5")
      valueMap("userRatingCount") = sample.getAs[Long]("userRatingCount").toString
      valueMap("userAvgReleaseYear") = sample.getAs[Int]("userAvgReleaseYear").toString
      valueMap("userReleaseYearStddev") = sample.getAs[String]("userReleaseYearStddev")
      valueMap("userAvgRating") = sample.getAs[String]("userAvgRating")
      valueMap("userRatingStddev") = sample.getAs[String]("userRatingStddev")

      redisClient.hset(userKey.toString, JavaConversions.mapAsJavaMap(valueMap).toString,"")
      insertedUserNumber += 1
      if (insertedUserNumber % 100 ==0){
        println(insertedUserNumber + "/" + userCount + "...")
      }
    }

    redisClient.close()
    userLatestSamples
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("featureEngineering")
      .set("spark.submit.deployMode", "client")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    //获取movies.csv的文件路径，并使用spark读取
    val movieResourcesPath = this.getClass.getResource("/data/movies.csv")
    println(movieResourcesPath.getPath)
    val movieSamples = spark.read.format("csv").option("header", "true").load(movieResourcesPath.getPath)

    //ratings.csv的文件路径，并使用spark读取
    val ratingsResourcesPath = this.getClass.getResource("/data/ratings.csv")
    val ratingSamples = spark.read.format("csv").option("header", "true").load(ratingsResourcesPath.getPath)

    //给ratingSample贴标签
    val ratingSamplesWithLabel = addSampleLabel(ratingSamples)
    //展示贴好标签的前十行
    ratingSamplesWithLabel.show(10, truncate = false)

    println("1")
    val samplesWithMovieFeatures = addMovieFeatures(movieSamples, ratingSamplesWithLabel)
    println("1111")

    val samplesWithUserFeatures = addUserFeatures(samplesWithMovieFeatures)

    println("2")
    //将得到的sample存储为csv格式
    splitAndSaveTrainingTestSamples(samplesWithUserFeatures, "/data")

    println("3")
    //将用户特征和电影特征存储到redis中
    extractAndSaveUserFeaturesToRedis(samplesWithUserFeatures)

    println("4")
    extractAndSaveMovieFeaturesToRedis(samplesWithUserFeatures)
    println("5")
    spark.close()
  }

}
