object PathConfig {
  // 数据库连接配置
  val DB_URL = "jdbc:mysql://192.168.1.100:3306/db_cartool_project"
  val USER_NAME = "root"
  val PASS_WORD = "123456"
  // HDFS路径
  val HDFS_PORT = "hdfs://192.168.1.100:9000"
  // 本地文件系统路径
  val LOCAL_PATH = "datas/data.txt"
  // 查询Sql
  val SELECT_SQL = "SELECT user_id uid,cartool_id kid ,COUNT(1) rating FROM `collect` GROUP BY user_id ,cartool_id;"
  // 删除sql
  val DELETE_SQL = "DELETE FROM rec"
}
