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
    // spark����
    val appName = "Recommend"
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    println("�����������......\n")
    loadDataFromDatabase(DB_URL, USER_NAME, PASS_WORD)
    println("�Ƽ�������գ�������д���ļ�......\n")

    // Spark��������
    val trainData = sc.textFile("datas/data.txt")
      .map { line =>
        val fields = line.split(",")
        (fields(0).toInt, fields(1).toInt, fields(2).toDouble)
      }
    println("���ݼ��سɹ�......\n")
    //����
    val weightedData = trainData.map { case (user, item, rating) =>
      Rating(user, item, rating)
    }
    //ѵ��ģ��
    val rank = 10
    val numIterations = 10
    val trainedModel = ALS.train(weightedData, rank, numIterations)
    println("ģ��ѵ�����......׼��д�����ݿ�.......\n")
    processRecommendations(trainedModel, 200, sc)
    sc.stop()
  }

  //д�����ݿ�
  def processRecommendations(model: MatrixFactorizationModel, recommendItemNum: Int, sc: SparkContext): Unit = {
    val userRecs = model.recommendProductsForUsers(recommendItemNum)
      .mapValues { recommendations =>
        recommendations.filter(_.rating >= 0.7)
      }

    userRecs.collect().foreach { case (user, recommendations) =>
      println("====================")
      println(s"�û�: $user")
      recommendations.foreach { case Rating(_, item, rating) =>
        println(s"����: $item, �Ƽ�ֵ: $rating")
      }
      println("====================")
    }
    JDBC.saveToDatabase(userRecs, DB_URL, USER_NAME, PASS_WORD)
  }

  def loadDataFromDatabase(dbUrl: String, username: String, password: String): Unit = {
    val connection: Connection = DriverManager.getConnection(dbUrl, username, password)
    // ����Ƽ���
    val deleteStatement: PreparedStatement = connection.prepareStatement(DELETE_SQL)
    deleteStatement.executeUpdate()
    deleteStatement.close()
    // ��ѯ����
    val selectStatement: PreparedStatement = connection.prepareStatement(SELECT_SQL)
    val resultSet: ResultSet = selectStatement.executeQuery()
    // д�����ݵ��ļ�
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
