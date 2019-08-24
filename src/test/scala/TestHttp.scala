import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.junit.Test

class TestHttp {

    @Test
    def testOne : Unit = {
        val client: CloseableHttpClient = HttpClients.createDefault()
        val get: HttpGet = new HttpGet("https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=2d24d3f8f2e10bca938db3886f690fc3")
        //
        val response: CloseableHttpResponse = client.execute(get)

        //获取返回结果
        val jsonAns: String = EntityUtils.toString(response.getEntity)

        println(jsonAns)
    }

}
