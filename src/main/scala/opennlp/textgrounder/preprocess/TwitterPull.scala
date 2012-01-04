package opennlp.textgrounder.preprocess

import net.liftweb.json
import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._

object TwitterPull {

  def force_value(value: json.JValue) : String = {
    (value values) toString
  }

  def parse_json(line: String): (String, String, String, Double, Double) = {
    val parsed = json.parse(line)
    val author = force_value(parsed \ "user" \ "screen_name")
    val timestamp = force_value(parsed \ "created_at")
    val text = force_value(parsed \ "text")
    val (lat, lng) = 
      if ((parsed \ "coordinate" values) == null ||
          (force_value(parsed \ "coordinate" \ "type") != "Point")) {
        (Double.NaN, Double.NaN)
      } else {
        val latlng: List[Double] = 
          (parsed \ "coordinate" \ "coordinate" values).asInstanceOf[List[Double]]
        (latlng(0), latlng(1))
      }
    (author, timestamp, text, lat, lng)
  }

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    val (inputPath, outputPath) =
      if (a.length == 2) {
        (a(0), a(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    // Firstly we load up all the (new-line-seperated) words into a DList
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val values_extracted = lines.map(parse_json)
    DList.persist(TextOutput.toTextFile(values_extracted, outputPath))

    /*
    // What we want to do, is record the frequency of words. So we'll convert it to a key-value
    // pairs where the key is the word, and the value the frequency (which to start with is 1)
    val keyValuePair: DList[(String, Int)] = lines flatMap { _.split(" ") } map { w => (w, 1) }

    // Now let's group all words that compare the same
    val grouped: DList[(String, Iterable[Int])] = keyValuePair.groupByKey
    // Now we have it in the form (Word, ['1', '1', '1', 1' etc.])

    // So what we want to do, is combine all the numbers into a single value (the frequency)
    val combined: DList[(String, Int)] = grouped.combine((_+_))

    // We can evalute this, and write it to a text file
    DList.persist(TextOutput.toTextFile(combined, outputPath + "/word-results"));
    */
  }

}
