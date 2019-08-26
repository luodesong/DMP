import com.luodesong.util.HbaseUtil.{HbaseNsUtil, HbasePool, HbaseTableDDLUtil}
import org.apache.hadoop.hbase.client.Connection
import org.junit.Test

class TestHbase {

    @Test
    def testOne: Unit = {
        //获取连接Connect
        val connection: Connection = HbasePool.getConnection
        println(connection)
        //2.获取HbaseAdmin对象
        val hbaseAdmin = connection.getAdmin
        //3.获取命令空间
        val namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors
        //4.获取所有的列表
        for (namespaceDescriptor <- namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName)
        }
        System.out.println(namespaceDescriptors.length + " row(s)")
        connection.close()
    }

    @Test
    def testTow: Unit = {
        //获取连接Connect
        val connection: Connection = HbasePool.getConnection
        println(connection)
        HbaseNsUtil.createNamespace(connection, "spark_dmp")
        connection.close()
    }

    @Test
    def testThree: Unit = {
        //获取连接Connect
        val connection: Connection = HbasePool.getConnection
        println(connection)
        HbaseTableDDLUtil.createTable(connection, "spark_dmp", "user_tags", "tags")
        connection.close()
    }

    @Test
    def testFour: Unit = {
        //获取连接Connect
        val connection: Connection = HbasePool.getConnection
        println(connection)
        HbaseTableDDLUtil.createTable(connection, "spark_dmp", "user_tags", "tags")
        connection.close()
    }
}
