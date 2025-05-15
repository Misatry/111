import PathConfig.{DB_URL, HDFS_PORT, LOCAL_PATH, PASS_WORD, USER_NAME, SELECT_SQL, DELETE_SQL}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, FileWriter}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object Recommend {
  def main(args: Array[String]): Unit = {
    // spark配置
    val appName = "Recommend"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("环境加载完成......\n")
    loadDataFromDatabase(DB_URL, USER_NAME, PASS_WORD)
    println("推荐表已清空，数据已写入文件......\n")

    // Spark加载数据
    val trainData = sc.textFile("datas/data.txt")
      .map { line =>
        val fields = line.split(",")
        (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    println("数据加载成功......\n")
    //特征
    val weightedData = trainData.map { case (user, item, rating) =>
      Rating(user, item, rating)
    }
    //训练模型
    val rank = 10
    val numIterations = 10
    val trainedModel = ALS.train(weightedData, rank, numIterations)
    println("模型训练完成......准备写入数据库.......\n")
    processRecommendations(trainedModel, 200, sc)
    sc.stop()
  }

  //写入数据库
  def processRecommendations(model: MatrixFactorizationModel, recommendItemNum: Int, sc: SparkContext): Unit = {
    val userRecs = model.recommendProductsForUsers(recommendItemNum)
      .mapValues { recommendations =>
        recommendations.filter(_.rating >= 0.7)
      }

    userRecs.collect().foreach { case (user, recommendations) =>
      println("====================")
      println(s"用户: $user")
      recommendations.foreach { case Rating(_, item, rating) =>
        println(s"动漫: $item, 推荐值: $rating")
      }
      println("====================")
    }
    JDBC.saveToDatabase(userRecs, DB_URL, USER_NAME, PASS_WORD)
  }

  def loadDataFromDatabase(dbUrl: String, username: String, password: String): Unit = {
    val connection: Connection = DriverManager.getConnection(dbUrl, username, password)
    // 清空推荐表
    val deleteStatement: PreparedStatement = connection.prepareStatement(DELETE_SQL)
    deleteStatement.executeUpdate()
    deleteStatement.close()
    // 查询数据
    val selectStatement: PreparedStatement = connection.prepareStatement(SELECT_SQL)
    val resultSet: ResultSet = selectStatement.executeQuery()
    // 写入数据到文件
    val writer = new BufferedWriter(new FileWriter(LOCAL_PATH))
    while (resultSet.next()) {
      val user = resultSet.getInt("uid")
      val item = resultSet.getInt("kid")
      val rating = resultSet.getDouble("rating")
      val line = s"$user,$item,$rating\n"
      writer.write(line)
    }
    writer.close()
    resultSet.close()
    selectStatement.close()
    connection.close()
  }
}
