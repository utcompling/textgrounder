package opennlp.textgrounder.preprocess

import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.HadoopLogFactory
// import com.nicta.scoobi.application.HadoopLogFactory
import org.apache.commons.logging.LogFactory
import java.io._

object ScoobiWordCount extends ScoobiApp {
  def run() {
    val lines = fromTextFile(args(0))

    def splitit(x: String) = {
      HadoopLogFactory.setQuiet(false)
      val logger = LogFactory.getLog("foo.bar")
      logger.info("Processing " + x)
      // System.err.println("Processing", x)
      x.split(" ")
    }
    //val counts = lines.flatMap(_.split(" "))
    val counts = lines.flatMap(splitit)
                          .map(word => (word, 1))
                          .groupByKey
                          .combine((a: Int, b: Int) => a + b)
    persist(toTextFile(counts, args(1)))
  }
}
