object PathConfig {
  // ���ݿ���������
  val DB_URL = "jdbc:mysql://192.168.1.100:3306/db_cartool_project"
  val USER_NAME = "root"
  val PASS_WORD = "123456"
  // HDFS·��
  val HDFS_PORT = "hdfs://192.168.1.100:9000"
  // �����ļ�ϵͳ·��
  val LOCAL_PATH = "datas/data.txt"
  // ��ѯSql
  val SELECT_SQL = "SELECT user_id uid,cartool_id kid ,COUNT(1) rating FROM `collect` GROUP BY user_id ,cartool_id;"
  // ɾ��sql
  val DELETE_SQL = "DELETE FROM rec"
}
