package opennlp.textgrounder.preprocess

import util.Random
import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._

/*
 * This program randomly permutes all the lines in a text file.
 */

object Permute {
  val rnd = new Random

  def generate_key(line: String): (Double, String) = {
    (rnd.nextDouble, line)
  }

  def remove_key(kvs: (Double, Iterable[String])): Iterable[String] = {
    val (key, values) = kvs
    for (v <- values)
      yield v
  }

  def main(args: Array[String]) = withHadoopArgs(args) { a =>
    // make sure we get all the input
    val (inputPath, outputPath) =
      if (a.length == 2) {
        (a(0), a(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    // Firstly we load up all the (new-line-seperated) json lines
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // randomly generate keys
    val with_keys = lines.map(generate_key)

    // sort by keys
    val keys_sorted = with_keys.groupByKey

    // remove keys
    val keys_removed = keys_sorted.flatMap(remove_key)

    // save to disk
    DList.persist(TextOutput.toTextFile(keys_removed, outputPath))

  }
}

