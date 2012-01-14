package opennlp.textgrounder.geolocate

import org.clapper.argot._
import java.io._

object GroupTweetsIntoDocuments {

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.geolocate.GroupTweetsIntoDocuments", preUsage = Some("TextGrounder"))
  val corpusFile = parser.option[String](List("i", "input"), "list", "corpus input file")
  val listFile = parser.option[String](List("l", "list"), "list", "list input file")
  val outDirPath = parser.option[String](List("o", "out"), "out", "output directory")
  
  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(corpusFile.value == None) {
      println("You must specify a corpus input file via -i.")
      sys.exit(0)
    }
    if(listFile.value == None) {
      println("You must specify a list input file via -l.")
      sys.exit(0)
    }
    if(outDirPath.value == None) {
      println("You must specify an output directory via -o.")
      sys.exit(0)
    }

    val outDir = if(outDirPath.value.get.endsWith("/")) outDirPath.value.get else outDirPath.value.get+"/"
    val dir = new File(outDir)
    if(!dir.exists)
      dir.mkdir

    val docNames:Set[String] = scala.io.Source.fromFile(listFile.value.get).getLines.
      map(_.split("\t")).map(p => p(0)).toSet

    /*var lines:Iterator[String] = null
    try {
      lines = scala.io.Source.fromFile(corpusFile.value.get, "utf-8").getLines
    } catch {
        case e: Exception => e.printStackTrace
    }*/

    val in = new BufferedReader(
      new InputStreamReader(new FileInputStream(corpusFile.value.get), "UTF8"))

    var line:String = in.readLine

    while(line != null) {
      val tokens = line.split("\t")
        if(tokens.length >= 6) {
          //println(line)
          val docName = tokens(0)
          val filename = outDir+docName
          if(docNames(docName)) {
            val tweet = tokens(5)
            val file = new File(filename)
            val out = 
              if(file.exists) new BufferedWriter(new FileWriter(filename, true))
              else new BufferedWriter(new FileWriter(filename))
            
            out.write(tweet+"\n")
            
            out.close
          }
        }

      line = in.readLine
    }

    in.close
  }
}
