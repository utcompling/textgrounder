import java.io._
import java.util._

import scala.collection.JavaConversions._

object CIAWFBFixer extends App {

  val countriesToCoords = new HashMap[String, (Double, Double)]

  var country = ""
  for(line <- scala.io.Source.fromFile(args(0)).getLines) {
    if(line.endsWith(":")) {
      country = line.dropRight(1).toLowerCase
      if(country.equals("korea, south"))
        country = "south korea"
      else if(country.equals("korea, north"))
        country = "north korea"
      else if(line.contains(",")) {
        country = line.slice(0, line.indexOf(",")).toLowerCase
      }
      //println(country)
    }

    if(line.length >= 5 && line.startsWith(" ")) {
      val tokens = line.trim.split("[^0-9SWNE]+")
      if(tokens.length >= 6) {
        val lat = (tokens(0).toDouble + tokens(1).toDouble / 60.0) * (if(tokens(2).equals("S")) -1 else 1)
        val lon = (tokens(3).toDouble + tokens(4).toDouble / 60.0) * (if(tokens(5).equals("W")) -1 else 1)
        countriesToCoords.put(country, (lat, lon))

        if(country.contains("bosnia"))
          countriesToCoords.put("bosnia", (lat, lon))

        if(country.equals("yugoslavia"))
          countriesToCoords.put("serbia", (lat, lon))

        if(country.equals("holy see (vatican city)"))
          countriesToCoords.put("vatican", (lat, lon))
      }
    }
  }

  countriesToCoords.put("montenegro", (42.5,19.1)) // from Google

  //countriesToCoords.foreach(p => println(p._1 + " " + p._2._1 + "," + p._2._2))

  val lineRE = """^(.*lat=\")([^\"]+)(.*long=\")(-0)(.*humanPath=\")([^\"]+)(.*)$""".r
  
  val inDir = new File(if(args(1).endsWith("/")) args(1).dropRight(1) else args(1))
  val outDir = new File(if(args(2).endsWith("/")) args(2).dropRight(1) else args(2))
  for(file <- inDir.listFiles.filter(_.getName.endsWith(".xml"))) {

    val out = new BufferedWriter(new FileWriter(outDir+"/"+file.getName))

    for(line <- scala.io.Source.fromFile(file).getLines) {
      if(line.contains("CIAWFB") && line.contains("long=\"-0\"")) {
        val lineRE(beg, lat0, mid, lon0, humpath, countryName, end) = line

        var lon = 0.0
        if(countriesToCoords.contains(countryName.toLowerCase)) {
          lon = countriesToCoords(countryName.toLowerCase)._2
        }

        var lat = lat0
        if(countryName.toLowerCase.equals("vatican"))
          lat = countriesToCoords(countryName.toLowerCase)._1.toString

        out.write(beg+lat+mid+lon+humpath+countryName+end+"\n")
        //println(beg+lat+mid+lon+humpath+countryName+end+"\n")
      }
      else
        out.write(line+"\n")
    }

    out.close
  }
  
}
