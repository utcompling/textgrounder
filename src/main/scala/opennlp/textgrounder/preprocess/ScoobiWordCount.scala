///////////////////////////////////////////////////////////////////////////////
//  ScoobiWordCount.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package preprocess

import com.nicta.scoobi.Scoobi._
// import com.nicta.scoobi.testing.HadoopLogFactory
import com.nicta.scoobi.application.HadoopLogFactory
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import java.io._

object ScoobiWordCount extends ScoobiApp {
  def run() {
    // There's some magic here in the source code to make the get() call
    // work -- there's an implicit conversion in object ScoobiConfiguration
    // from a ScoobiConfiguration to a Hadoop Configuration, which has get()
    // defined on it.  Evidently implicit conversions in the companion object
    // get made available automatically for classes or something?
    System.err.println("mapred.job.tracker " +
      configuration.get("mapred.job.tracker", "value not found"))
    // System.err.println("job tracker " + jobTracker)
    // System.err.println("file system " + fs)
    System.err.println("configure file system " + configuration.fs)
    System.err.println("file system key " +
      configuration.get(FileSystem.FS_DEFAULT_NAME_KEY, "value not found"))

    val lines =
      // Test fromTextFileWithPath, but currently appears to trigger an
      // infinite loop.
      // TextInput.fromTextFileWithPath(args(0))
      TextInput.fromTextFile(args(0)).map(x => (args(0), x))

    def splitit(x: String) = {
      HadoopLogFactory.setQuiet(false)
      // val logger = LogFactory.getLog("foo.bar")
      // logger.info("Processing " + x)
      // System.err.println("Processing", x)
      x.split(" ")
    }
    //val counts = lines.flatMap(_.split(" "))
    val counts = lines.map(_._2).flatMap(splitit)
                          .map(word => (word, 1))
                          .groupByKey
                          .filter { case (word, lens) => word.length < 8 }
                          .filter { case (word, lens) => lens.exists(x => true) }
                          .combine((a: Int, b: Int) => a + b)
    persist(toTextFile(counts, args(1)))
  }
}
