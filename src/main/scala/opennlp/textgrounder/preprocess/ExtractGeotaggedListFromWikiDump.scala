///////////////////////////////////////////////////////////////////////////////
//  PreprocWikiDump.scala
//
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder.preprocess

import opennlp.textgrounder.tr.topo._

import java.io._
import org.apache.commons.compress.compressors.bzip2._

object ExtractGeotaggedListFromWikiDump {

  var MAX_COUNT = 0
  //val NEW_PAGE = "    <title>"
  //val coordRE = """\|\s*latd|\|\s*lat_deg|\|\s*latG|\|\s*latitude|\{\{\s*Coord?|\|\s*Breitengrad|\{\{\s*Coordinate\s""".r

  val titleRE = """^\s{4}<title>(.*)</title>\s*$""".r
  val redirectRE = """^\s{4}<redirect title=\"(.*)\" />\s*$""".r
  val idRE = """^\s{4}<id>(.*)</id>\s*$""".r

  val coordRE = """^.*\{\{\s*(?:[Cc]oord|[Cc]oordinate)\s*\|.*$""".r
  val coord_decimal = """^.*\{\{\s*(?:[Cc]oord|[Cc]oordinate)\s*\|\s*(-?\d+\.?\d*+)\s*\|\s*(-?\d+\.?\d*+)\s*\|.*$""".r
  val coord_dms = """^.*\{\{\s*(?:[Cc]oord|[Cc]oordinate)?\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([Nn]|[Ss])\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([Ee]|[Ww])\s*.*$""".r
  val coord_dm = """^.*\{\{\s*(?:[Cc]oord|[Cc]oordinate)?\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([Nn]|[Ss])\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|\s*([Ee]|[Ww])\s*.*$""".r
  val coord_d = """^.*\{\{\s*(?:[Cc]oord|[Cc]oordinate)?\s*\|\s*(-?\d+\.?\d*+)\s*\|\s*([Nn]|[Ss])\s*\|\s*(-?\d+\.?\d*+)\s*\|\s*([Ee]|[Ww])\s*.*$""".r
  val latRE = """^.*[Ll]atd\s*=\s*(\d+)\s*\|.*$""".r
  val latdms = """^.*[Ll]atd\s*=\s*(\d+)\s*\|\s*[Ll]atm\s*=(\d+)\s*\|\s*[Ll]ats\s*=(\d+)\s*\|\s*[Ll]at[Nn][Ss]\s*=\s*([Nn]|[Ss])\s*\|\s*[Ll]ongd\s*=\s*(\d+)\s*\|\s*[Ll]ongm\s*=\s*(\d+)\s*\|\s*[Ll]ongs\s*=\s*(\d+)\s*\|\s*[Ll]ong[Ee][Ww]\s*=\s*([Ee]|[Ww])\s*.*$""".r
  val latdm = """^.*[Ll]atd\s*=\s*(\d+)\s*\|\s*[Ll]atm\s*=(\d+)\s*\|\s*[Ll]at[Nn][Ss]\s*=\s*([Nn]|[Ss])\s*\|\s*[Ll]ongd\s*=\s*(\d+)\s*\|\s*[Ll]ongm\s*=\s*(\d+)\s*\|\s*[Ll]ong[Ee][Ww]\s*=\s*([Ee]|[Ww])\s*.*$""".r
  val latd = """^.*[Ll]atd\s*=\s*(-?\d+\.?\d*+)\s*\|\s*[Ll]at[Nn][Ss]\s*=\s*([Nn]|[Ss])\s*\|\s*[Ll]ongd\s*=\s*(-?\d+\.?\d*+)\s*\|\s*[Ll]ong[Ee][Ww]\s*=\s*([Ee]|[Ww])\s*.*$""".r

  def main(args: Array[String]) {
    val fileInputStream = new FileInputStream(new File(args(0)))
    if(args.length >= 2)
      MAX_COUNT = args(1).toInt
    //fileInputStream.read(); // used to be null pointer without this
    //fileInputStream.read();
    val cbzip2InputStream = new BZip2CompressorInputStream(fileInputStream)
    val in = new BufferedReader(new InputStreamReader(cbzip2InputStream))

    val redirectsOut = new BufferedWriter(new FileWriter("redirects.txt"))

    var totalPageCount = 0
    var geotaggedPageCount = 0
    var lookingForCoord = false
    var lineCount = 0
    var title = ""
    //var redirectTitle = ""
    var id = ""
    var line = in.readLine
    while(line != null && (MAX_COUNT <= 0 || lineCount < MAX_COUNT)) {

      if(titleRE.findFirstIn(line) != None) {
        val titleRE(t) = line
        title = t
        totalPageCount += 1
        /*if(totalPageCount % 10000 == 0)
          println(line+" "+geotaggedPageCount+"/"+totalPageCount)*/
        lookingForCoord = true
        //redirectTitle = null
      }

      if(redirectRE.findFirstIn(line) != None) {
        val redirectRE(r) = line
        redirectsOut.write(title+"\t"+r+"\n")
      }

      if(idRE.findFirstIn(line) != None) {
        val idRE(i) = line
        id = i
      }

      //if(lookingForCoord) {// && coordRE.findFirstIn(line) != None) {
        //println(title)
        //println(line)
        //geotaggedPageCount += 1
        //lookingForCoord = false

      var coord:Coordinate = null
        
      if(lookingForCoord) {
        if(coordRE.findFirstIn(line) != None) {
          coord = line match {
            case coord_decimal(lat,lon) => { Coordinate.fromDegrees(lat.toDouble, lon.toDouble) }
            case coord_dms(latd, latm, lats, ns, longd, longm, longs, ew) => {
              val lat = (if(ns.equalsIgnoreCase("S")) -1 else 1) * latd.toDouble + latm.toDouble/60 + lats.toDouble/3600
              val lon = (if(ew.equalsIgnoreCase("W")) -1 else 1) * longd.toDouble + longm.toDouble/60 + longs.toDouble/3600
              Coordinate.fromDegrees(lat, lon)
            }
            case coord_dm(latd, latm, ns, longd, longm, ew) => {
              val lat = (if(ns.equalsIgnoreCase("S")) -1 else 1) * latd.toDouble + latm.toDouble/60
              val lon = (if(ew.equalsIgnoreCase("W")) -1 else 1) * longd.toDouble + longm.toDouble/60
              Coordinate.fromDegrees(lat, lon)
            }
            case coord_d(lat,ns, lon, ew) => { Coordinate.fromDegrees((if(ns.equalsIgnoreCase("S")) -1 else 1)*lat.toDouble, (if(ew.equalsIgnoreCase("W")) -1 else 1)*lon.toDouble) }
            case _ => null
          }
        }
      
        else if(latRE.findFirstIn(line) != None) {
          coord = line match {
            case latdms(latd, latm, lats, ns, longd, longm, longs, ew) => {
              val lat = (if(ns.equalsIgnoreCase("S")) -1 else 1) * latd.toDouble + latm.toDouble/60 + lats.toDouble/3600
              val lon = (if(ew.equalsIgnoreCase("W")) -1 else 1) * longd.toDouble + longm.toDouble/60 + longs.toDouble/3600
              Coordinate.fromDegrees(lat, lon)
            }
            case latdm(latd, latm, ns, longd, longm, ew) => {
              val lat = (if(ns.equalsIgnoreCase("S")) -1 else 1) * latd.toDouble + latm.toDouble/60
              val lon = (if(ew.equalsIgnoreCase("W")) -1 else 1) * longd.toDouble + longm.toDouble/60
              Coordinate.fromDegrees(lat, lon)
            }
            case latd(latd, ns, longd, ew) => {
              val lat = (if(ns.equalsIgnoreCase("S")) -1 else 1) * latd.toDouble
              val lon = (if(ew.equalsIgnoreCase("W")) -1 else 1) * longd.toDouble
              Coordinate.fromDegrees(lat, lon)
            }
            case _ => null
          }
        }

        if(coord != null) {
          println(id+"\t"+title+"\t"+coord)
          geotaggedPageCount += 1
          lookingForCoord = false
        }
      }
      
      line = in.readLine
      lineCount += 1
    }

    //println("Geotagged page count: "+geotaggedPageCount)
    //println("Total page count: "+totalPageCount)

    redirectsOut.close
    in.close
  }
}
