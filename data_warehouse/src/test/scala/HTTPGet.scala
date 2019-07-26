import java.io.InputStream
import java.util

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

/*
 @author Daniel51
 @DESCRIPTION ${DESCRIPTION}
 @create 2019/7/25
*/
object HTTPGet {
  def main(args: Array[String]): Unit = {


    val client: CloseableHttpClient = HttpClientBuilder.create().build()

    val string="https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=42d4e3f075e2eacbefb6a247afd8f35d&radius=1000&extensions=al"
    val get = new HttpGet(string)
    val response: CloseableHttpResponse = client.execute(get)
    val content: InputStream = response.getEntity.getContent
    val strings: util.List[String] = IOUtils.readLines(content)
//    import scala.collection.JavaConversons._
//    println(strings.mkString)
  }

}
