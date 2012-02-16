package opennlp.textgrounder.preprocess

import java.io._
import org.apache.tools.bzip2._

object PreprocWikiDump {

  val MAX_COUNT = 200000
  val NEW_PAGE = "    <title>"
  val coordRE = """\|\s*latd|\|\s*lat_deg|\|\s*latG|\|\s*latitude|\{\{\s*Coord?|\|\s*Breitengrad|\{\{\s*Coordinate\s""".r

  def main(args: Array[String]) {
    val fileInputStream = new FileInputStream(new File(args(0)))
    fileInputStream.read(); // otherwise null pointer
    fileInputStream.read();
    val cbzip2InputStream = new CBZip2InputStream(fileInputStream)
    val in = new BufferedReader(new InputStreamReader(cbzip2InputStream))

    var totalPageCount = 0
    var geotaggedPageCount = 0
    var lookingForCoord = false
    var lineCount = 0
    //var title = ""
    var line = in.readLine
    while(line != null /*&& lineCount < MAX_COUNT*/) {

      //println(line)
      if(line.startsWith(NEW_PAGE)) {
        //title = line
        totalPageCount += 1
        if(totalPageCount % 10000 == 0)
          println(line+" "+geotaggedPageCount+"/"+totalPageCount)
        lookingForCoord = true
      }

      if(lookingForCoord && coordRE.findFirstIn(line) != None) {
        //println(title)
        //println(line)
        geotaggedPageCount += 1
        lookingForCoord = false
      }
      
      line = in.readLine
      lineCount += 1
    }

    println("Geotagged page count: "+geotaggedPageCount)
    println("Total page count: "+totalPageCount)

    in.close
  }
}
