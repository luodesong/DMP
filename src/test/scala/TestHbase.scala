import com.luodesong.util.HbasePool
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.client.{Admin, Connection}
import org.junit.Test

class TestHbase {

    @Test
    def testOne: Unit ={
        val connection: Connection = HbasePool.getConnection
        println(connection)
        //2.获取HbaseAdmin对象
        val hbaseAdmin = connection.getAdmin
        val namespaceDescriptors = hbaseAdmin.listNamespaceDescriptors
        for (namespaceDescriptor <- namespaceDescriptors) {
            System.out.println(namespaceDescriptor.getName)
        }
        System.out.println(namespaceDescriptors.length + " row(s)")
        connection.close()
    }
}
