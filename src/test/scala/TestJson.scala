import com.luodesong.util.MapUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TestJson {
    @Test
    def testOne : Unit ={
        val conf: SparkConf = new SparkConf()
                .setAppName(this.getClass.getName)
                .setMaster("local[*]")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val sc: SparkContext = new SparkContext(conf)
        val sqlContext: Unit = new SQLContext(sc)
                .setConf("spark.io.compression.snappy.codec", "snappy")
        val str: String = MapUtil.getBusinessFromAmap(0,0)
        println(str)
    }

}
