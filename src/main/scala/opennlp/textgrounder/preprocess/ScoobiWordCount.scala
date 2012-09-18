package opennlp.textgrounder.preprocess

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.HadoopLogFactory
// import com.nicta.scoobi.application.HadoopLogFactory
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import java.io._

object ScoobiWordCount extends ScoobiApp {
  implicit def toPimpedDlist[K,V](dl: DList[(K, Iterable[V])]) = new 
  PimpedDList(dl) 
  class PimpedDList[K,V](dl: DList[(K,Iterable[V])]) { 
      def safeCombine(f: (V, V) => V) 
        (implicit mK: Manifest[K], 
         wtK: WireFormat[K], 
         grpK: Grouping[K], 
         mV: Manifest[V], 
         wtV: WireFormat[V]): DList[(K, V)] = dl.map { case (k, vs) => 
  (k, vs.reduce(f)) } 
    } 

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
                          .filter { case (word, len) => word.length < 8 }
                          .safeCombine((a: Int, b: Int) => a + b)
    persist(toTextFile(counts, args(1)))
  }
}
