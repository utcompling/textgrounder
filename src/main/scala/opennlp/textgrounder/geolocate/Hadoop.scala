///////////////////////////////////////////////////////////////////////////////
//  Hadoop.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.geolocate

import collection.JavaConversions._

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment.ExperimentMeteredTask
import opennlp.textgrounder.util.hadoop._
import opennlp.textgrounder.util.ioutil.FileHandler
import opennlp.textgrounder.util.mathutil.{mean, median}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import opennlp.textgrounder.gridlocate.{CellGridEvaluator,TextGrounderInfo,DistDocumentFileProcessor}

/* Basic idea for hooking up Geolocate with Hadoop.  Hadoop works in terms
   of key-value pairs, as follows:

   (1) A preprocessor generates key-value pairs, which are passed to hadoop.
       Note that typically at this stage what's passed to Hadoop is not
       in the form of a key-value pair but just some sort of item, e.g. a
       line of text.  This typically becomes the value of the key-value pair,
       while something that most programs ignore becomes the key (e.g. the
       item count of the item that was seen).  Note that these keys and values,
       as for all data passed around by Hadoop, is typed, and the type is
       under the programmer's control.  Hence, although the value is commonly
       text, it may not be.

   (2) Hadoop creates a number of mappers and partitions the input items in
       some way, passing some fraction of the input items to each mapper.

   (3) Each mapper iterates over its input items, and for each in turn,
       generates a possibly-empty set of key-value output items.  Note that
       the types of the output keys and values may be totally different from
       those of the input keys and values.  The output key has much more
       significance than the input key.

   (5) A "shuffle" step happens internally, where all output items are
       grouped according to their keys, and the keys are further sorted.
       (Or equivalently, the entire set of output items is sorted on their
       keys, which will naturally place identical keys next to each other
       as well as ensuring that non-identical keys are sorted, and then
       sets of items with identical keys are transformed into single items
       with the same key and a value consisting of a list of all of items
       grouped together.) What actually happens is that the items are
       sorted and grouped at the end of the map stage on the map node,
       *before* being sent over the network; the overall "shuffle" then
       simply involves merging.
   
   (4.5) To reduce the amount of data sent over the network, a combiner can
         be defined, which runs on the map node after sorting but before
         sending the data over.  This is optional, and if it exists, does
         a preliminary reduce.  Depending on the task in question, this may
         be exactly the same as the reducer.  For example, if the reducer
         simply adds up all of the items passed to it, the same function
         can be used as a combiner, since a set of number can be added up
         all at once or in parts. (An example of where this can't be done
         is when the reducer needs to find the median of a set of items.
         Computing the median involves selecting one of the items of a
         set rather than mashing them all together, and which item is to
         be selected cannot be known until the entire set is seen.  Given
         a subset, the median could be any value in the subset; hence the
         entire subset must be sent along, and cannot in general be
         "combined" in any way.)
         
   (6) A set of reducers are created, and the resulting grouped items are
       partitioned based on their keys, with each reducer receiving one
       sorted partition, i.e. a list of all the items whose keys were
       assigned to that reducer, in sorted order, where the value of each
       item (remember, items are key-value pairs) is a list of items
       (all values associated with the same key in the items output by
       the mapper).  Each key is seen only once (assuming no crashes/restarts),
       and only on a single reducer.  The reducer then outputs its own
       output pairs, typically by "reducing" the value list of each key
       into a single item.

   (7) A post-processor might take these final output items and do something
       with them.

   Note about types:

   In general:

   MAP STAGE:

   Mapper input is of type A -> B
   Mapper output is of type C -> D
   Hence map() is of type (A -> B) -> Iterable[(C -> D)]

   Often types B and C are identical or related.

   COMBINE STAGE:

   The combiner is strictly an optimization, and the program must work
   correctly regardless of whether the combiner is run or not -- or, for
   that matter, if run multiple times.  This means that the input and
   output types of the combiner must be the same, and in most cases
   the combiner must be idempotent (i.e. if its input is a previous
   output, it should output is input unchanged; in other words, it
   does nothing if run multiple times on the same input).

   Combiner input is of type C -> Iterable[D]
   Combiner output is of type C -> Iterable[D]
   Hence combine() is of type (C -> Iterable[D]) -> (C -> Iterable[D])

   (The output of the cominber is grouped, just like its input from the
   map output.)

   REDUCE STAGE:

   Reducer input is of type C -> Iterable[D]
   Reducer output is of type E -> F
   Hence reduce() is of type (C -> Iterable[D]) -> (E -> F)

   In our case, we assume that the mappers() do the real work and the
   reducers just collect the stats and combine them.  We can break a
   big job in two ways: Either by partitioning the set of test documents
   and having each mapper do a full evaluation on a limited number of
   test documents, or by partitioning the grid and have each mapper
   compare all test documents against a portion of the grid.  A third
   possibility is to combine both, where a mapper does a portion of
   the test documents against a portion of the grid.

   OUR IMPLEMENTATION:

   Input values to map() are tuples (strategy, document).  Output items
   are have key = (cellgrid-details, strategy), value = result for
   particular document (includes various items, including document,
   predicted cell, true rank, various distances).  No combiner, since
   we have to compute a median, meaning we need all values.  Reducer
   computes mean/median for all values for a given cellgrid/strategy.
   NOTE: For identifying a particular cell, we use indices, since we
   can't pass pointers.  For KD trees and such, we conceivably might have
   to pass in to the reducer some complex details identifying the
   cell grid parameters.  If so, this probably would get passed first
   to all reducers using the trick of creating a custom partitioner
   that ensures the reducer gets this info first.
*/

/************************************************************************/
/*                  General Hadoop code for Geolocate app               */
/************************************************************************/
   
abstract class HadoopGeolocateApp(
  progname: String
) extends GeolocateApp(progname) with HadoopCorpusApp {
  override type TDriver <: HadoopGeolocateDriver

  def corpus_suffix =
    driver.params.eval_set + "-" + driver.document_file_suffix
  def corpus_dirs = params.input_corpus

  override def initialize_hadoop_input(job: Job) {
    super.initialize_hadoop_input(job)
    FileOutputFormat.setOutputPath(job, new Path(params.outfile))
  }
}

trait HadoopGeolocateParameters extends GeolocateParameters {
  var textgrounder_dir =
    ap.option[String]("textgrounder-dir",
      help = """Directory to use in place of TEXTGROUNDER_DIR environment
variable (e.g. in Hadoop).""")

  var outfile =
    ap.positional[String]("outfile",
      help = """File to store evaluation results in.""")

}

/**
 * Base mix-in for a Geolocate application using Hadoop.
 *
 * @see HadoopGeolocateDriver
 */

trait HadoopGeolocateDriver extends
    GeolocateDriver with HadoopExperimentDriver {
  override type TParam <: HadoopGeolocateParameters

  override def handle_parameters() {
    super.handle_parameters()
    need(params.textgrounder_dir, "textgrounder-dir")
    TextGrounderInfo.set_textgrounder_dir(params.textgrounder_dir)
  }
}

/************************************************************************/
/*                Hadoop implementation of geolocate-document           */
/************************************************************************/

class DocumentEvaluationMapper extends
    Mapper[Object, Text, Text, DoubleWritable] with
    HadoopExperimentMapReducer {
  def progname = HadoopGeolocateDocumentApp.progname
  type TContext = Mapper[Object, Text, Text, DoubleWritable]#Context
  type TDriver = HadoopGeolocateDocumentDriver
  // more type erasure crap
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver

  var evaluators: Iterable[CellGridEvaluator[SphereCoord,SphereDocument,_,_,_]] = null
  val task = new ExperimentMeteredTask(driver, "document", "evaluating")

  class HadoopDocumentFileProcessor(
    context: TContext
  ) extends DistDocumentFileProcessor(
    driver.params.eval_set + "-" + driver.document_file_suffix, driver
  ) {
    override def get_shortfile =
      filename_to_counter_name(driver.get_file_handler,
        driver.get_configuration.get("mapred.input.dir"))

    /* #### FIXME!!! Need to redo things so that different splits are
       separated into different files. */
    def handle_document(fieldvals: Seq[String]) = {
      val table = driver.document_table
      val doc = table.create_and_init_document(schema, fieldvals, false)
      val retval = if (doc != null) {
        doc.dist.finish_after_global()
        var skipped = 0
        var not_skipped = 0
        for (e <- evaluators) {
          val num_processed = task.num_processed
          val doctag = "#%d" format (1 + num_processed)
          if (e.would_skip_document(doc, doctag)) {
            skipped += 1
            errprint("Skipped document %s because evaluator would skip it",
              doc)
          } else {
            not_skipped += 1
            // Don't put side-effecting code inside of an assert!
            val result =
              e.evaluate_document(doc, doctag)
            assert(result != null)
            context.write(new Text(e.stratname),
              new DoubleWritable(result.asInstanceOf[SphereDocumentEvaluationResult].pred_truedist))
            task.item_processed()
          }
          context.progress
        }
        if (skipped > 0 && not_skipped > 0)
          warning("""Something strange: %s evaluator(s) skipped document, but %s evaluator(s)
didn't skip.  Usually all or none should skip.""", skipped, not_skipped)
        (not_skipped > 0)
      } else false
      context.progress
      (retval, true)
    }

    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String) =
      throw new IllegalStateException(
        "process_lines should never be called here")
  }

  var processor: HadoopDocumentFileProcessor = _
  override def init(context: TContext) {
    super.init(context)
    if (driver.params.eval_format != "internal")
      driver.params.parser.error(
        "For Hadoop, '--eval-format' must be 'internal'")
    else {
      evaluators =
        for ((stratname, strategy) <- driver.strategies)
          yield driver.create_document_evaluator(strategy, stratname).
            asInstanceOf[CellGridEvaluator[
              SphereCoord,SphereDocument,_,_,_]]
      if (driver.params.input_corpus.length != 1) {
        driver.params.parser.error(
          "FIXME: For Hadoop, currently need exactly one corpus")
      } else {
        processor = new HadoopDocumentFileProcessor(context)
        processor.read_schema_from_corpus(driver.get_file_handler,
            driver.params.input_corpus(0))
        context.progress
      }
    }
  }

  override def setup(context: TContext) { init(context) }

  override def map(key: Object, value: Text, context: TContext) {
    processor.parse_row(value.toString)
    context.progress
  }
}

class DocumentResultReducer extends
    Reducer[Text, DoubleWritable, Text, DoubleWritable] {

  type TContext = Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context

  var driver: HadoopGeolocateDocumentDriver = _

  override def setup(context: TContext) {
    driver = new HadoopGeolocateDocumentDriver
    driver.set_task_context(context)
  }

  override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable],
      context: TContext) {
    val errordists = (for (v <- values) yield v.get).toSeq
    val mean_dist = mean(errordists)
    val median_dist = median(errordists)
    context.write(new Text(key.toString + " mean"), new DoubleWritable(mean_dist))
    context.write(new Text(key.toString + " median"), new DoubleWritable(median_dist))
  }
}

class HadoopGeolocateDocumentParameters(
  parser: ArgParser = null
) extends GeolocateDocumentParameters(parser) with HadoopGeolocateParameters {
}

/**
 * Class for running the geolocate-document app using Hadoop.
 */

class HadoopGeolocateDocumentDriver extends
    GeolocateDocumentTypeDriver with HadoopGeolocateDriver {
  override type TParam = HadoopGeolocateDocumentParameters
}

object HadoopGeolocateDocumentApp extends
    HadoopGeolocateApp("TextGrounder geolocate-document") {
  type TDriver = HadoopGeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()

  def initialize_hadoop_classes(job: Job) {
    job.setJarByClass(classOf[DocumentEvaluationMapper])
    job.setMapperClass(classOf[DocumentEvaluationMapper])
    job.setReducerClass(classOf[DocumentResultReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[DoubleWritable])
  }
}

// Old code.  Probably won't ever be needed.  If we feel the need to move
// to more complex types when serializing, we should switch to Avro rather
// than reinventing the wheel.

// /**
//  * Hadoop has a standard Writable class but it isn't so good for us, since
//  * it assumes its read method
//  */
// trait HadoopGeolocateWritable[T] {
//   def write(out: DataOutput): Unit
//   def read(in: DataInput): T
// }
// 
// /**
//  * Class for writing out in a format suitable for Hadoop.  Implements
//    Hadoop's Writable interface.  Because the 
//  */
// 
// abstract class RecordWritable() extends WritableComparable[RecordWritable] {
// }

/*

abstract class ObjectConverter {
  type Type
  type TWritable <: Writable
  def makeWritable(): TWritable
  def toWritable(obj: Type, w: TWritable)
  def fromWritable(w: TWritable): obj
}

object IntConverter {
  type Type = Int
  type TWritable = IntWritable
 
  def makeWritable() = new IntWritable
  def toWritable(obj: Int, w: IntWritable) { w.set(obj) }
  def fromWritable(w: TWritable) = w.get
}

abstract class RecordWritable(
  fieldtypes: Seq[Class]
) extends WritableComparable[RecordWritable] {
  type Type

  var obj: Type = _
  var obj_set: Boolean = false

  def set(xobj: Type) {
    obj = xobj
    obj_set = true
  }

  def get() = {
    assert(obj_set)
    obj
  }

  def write(out: DataOutput) {}
  def readFields(in: DataInput) {}

  val writables = new Array[Writable](fieldtypes.length)
}


object SphereDocumentConverter extends RecordWriterConverter {
  type Type = SphereDocument

  def serialize(doc: SphereDocument) = doc.title
  def deserialize(title: String) = FIXME

  def init() {
    RecordWriterConverter.register_converter(SphereDocument, this)
  }
}


class DocumentEvaluationResultWritable extends RecordWritable {
  type Type = DocumentEvaluationResult
  def to_properties(obj: Type) =
    Seq(obj.document, obj.pred_cell, obj.true_rank,
        obj.true_cell, obj.num_docs_in_true_cell,
        obj.true_center, obj.true_truedist, obj.true_degdist,
        obj.pred_center, obj.pred_truedist, obj.pred_degdist)
  def from_properties(props: Seq[Any]) = {
    val Seq(document, pred_cell, true_rank,
        true_cell, num_docs_in_true_cell,
        true_center, true_truedist, true_degdist,
        pred_center, pred_truedist, pred_degdist) = props
    new HadoopDocumentEvaluationResult(
      document.asInstanceOf[SphereDocument],
      pred_cell.asInstanceOf[GeoCell],
      true_rank.asInstanceOf[Int],
      true_cell.asInstanceOf[GeoCell],
      num_docs_in_true_cell.asInstanceOf[Int],
      true_center.asInstanceOf[SphereCoord],
      true_truedist.asInstanceOf[Double],
      true_degdist.asInstanceOf[Double],
      pred_center.asInstanceOf[SphereCoord],
      pred_truedist.asInstanceOf[Double],
      pred_degdist.asInstanceOf[Double]
    )
  }
}

*/
