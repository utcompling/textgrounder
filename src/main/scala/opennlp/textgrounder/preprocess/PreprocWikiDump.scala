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

import java.io._
import org.apache.commons.compress.compressors.bzip2._

/*
 * DOCUMENT ME!  Commit message says "for use with Mallet in getting topics
 * out of certain subsets of documents."
 */
object PreprocWikiDump {

  val MAX_COUNT = 200000
  val NEW_PAGE = "    <title>"
  val coordRE = """\|\s*latd|\|\s*lat_deg|\|\s*latG|\|\s*latitude|\{\{\s*Coord?|\|\s*Breitengrad|\{\{\s*Coordinate\s""".r

  def main(args: Array[String]) {
    val fileInputStream = new FileInputStream(new File(args(0)))
    fileInputStream.read(); // otherwise null pointer
    fileInputStream.read();
    val cbzip2InputStream = new BZip2CompressorInputStream(fileInputStream)
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
