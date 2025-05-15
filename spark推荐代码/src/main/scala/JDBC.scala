import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import java.sql.{Connection, DriverManager, PreparedStatement}
object JDBC {
  def saveToDatabase(userRecs: RDD[(Int, Array[Rating])], dbUrl: String, username: String, password: String): Unit = {
    userRecs.foreach { case (user, recommendations) =>
      val connection: Connection = DriverManager.getConnection(dbUrl, username, password)
      val statement: PreparedStatement = connection.prepareStatement("INSERT INTO rec (user_id, car_tool_id,score) VALUES (?, ?,?)")
      recommendations.foreach { case Rating(_, item, rating) =>
        statement.setInt(1, user)
        statement.setInt(2, item)
        statement.setDouble(3, rating)
        statement.executeUpdate()
      }
      statement.close()
      connection.close()
    }
  }
}