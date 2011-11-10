package opennlp.textgrounder.geolocate

import tgutil._
import hadooputil._
import argparser._

import org.apache.hadoop.io._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs._

import java.io.{FileSystem=>_,_}
import collection.JavaConversions._
import util.control.Breaks._

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

   Input values to map() are tuples (strategy, article).  Output items
   are have key = (cellgrid-details, strategy), value = result for
   particular article (includes various items, including article,
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
   
object GeolocateHadoopConfiguration {
  /* Prefix used for storing parameters in a Hadoop configuration */
  val hadoop_conf_prefix = "textgrounder."

  /**
   * Convert the parameters in `parser` to Hadoop configuration settings in
   * `conf`.
   *
   * @param prefix Prefix to prepend to the names of all parameters.
   * @param parser ArgParser object to retrieve parameters from.
   * @param conf Configuration object to store configuration settings into.
   */
  def convert_parameters_to_hadoop_conf(prefix: String, parser: ArgParser,
      conf: Configuration) {
    for (name <- parser.argNames if parser.specified(name)) {
      val confname = prefix + name
      parser(name) match {
        case e:Int => conf.setInt(confname, e)
        case e:Long => conf.setLong(confname, e)
        case e:Float => conf.setFloat(confname, e)
        case e:Double => conf.setFloat(confname, e.toFloat)
        case e:String => conf.set(confname, e)
        case e:Boolean => conf.setBoolean(confname, e)
        case e:Seq[_] => {
          val multitype = parser.getMultiType(name)
          if (multitype == classOf[String]) {
            conf.setStrings(confname, parser.get[Seq[String]](name): _*)
          } else
            throw new UnsupportedOperationException(
              "Don't know how to store sequence of type %s of parameter %s into a Hadoop Configuration"
              format (multitype, name))
        }
        case ty@_ => {
          throw new UnsupportedOperationException(
            "Don't know how to store type %s of parameter %s into a Hadoop Configuration"
            format (ty, name))
        }
      }
    }
  }

  /**
   * Convert the relevant Hadoop configuration settings in `conf`
   * into the given ArgParser.
   *
   * @param prefix Prefix to prepend to the names of all parameters.
   * @param parser ArgParser object to store parameters into.  The names
   *   of parameters to fetch are taken from this object.
   * @param conf Configuration object to retrieve settings from.
   */
  def convert_parameters_from_hadoop_conf(prefix: String, parser: ArgParser,
      conf: Configuration) {
    // Configuration.dumpConfiguration(conf, new PrintWriter(System.err))
    for {name <- parser.argNames
         confname = prefix + name
         if conf.getRaw(confname) != null} {
      val confname = prefix + name
      val ty = parser.getType(name)
      if (ty == classOf[Int])
        parser.set[Int](name, conf.getInt(confname, parser.defaultValue[Int](name)))
      else if (ty == classOf[Long])
        parser.set[Long](name, conf.getLong(confname, parser.defaultValue[Long](name)))
      else if (ty == classOf[Float])
        parser.set[Float](name, conf.getFloat(confname, parser.defaultValue[Float](name)))
      else if (ty == classOf[Double])
        parser.set[Double](name, conf.getFloat(confname, parser.defaultValue[Double](name).toFloat).toDouble)
      else if (ty == classOf[String])
        parser.set[String](name, conf.get(confname, parser.defaultValue[String](name)))
      else if (ty == classOf[Boolean])
        parser.set[Boolean](name, conf.getBoolean(confname, parser.defaultValue[Boolean](name)))
      else if (ty == classOf[Seq[_]]) {
        val multitype = parser.getMultiType(name)
        if (multitype == classOf[String])
          parser.set[Seq[String]](name, conf.getStrings(confname, parser.defaultValue[Seq[String]](name): _*).toSeq)
        else
          throw new UnsupportedOperationException(
            "Don't know how to fetch sequence of type %s of parameter %s from a Hadoop Configuration"
            format (multitype, name))
      } else {
        throw new UnsupportedOperationException(
          "Don't know how to store fetch %s of parameter %s from a Hadoop Configuration"
          format (ty, name))
      }
    }
  }
}

class ArticleEvaluationMapper extends
    Mapper[Object, Text, Text, DoubleWritable] {
  val reader = new ArticleReader(ArticleData.combined_article_data_outfields)
  var evaluators: Iterable[ArticleGeolocateDocumentEvaluator] = null
  val task = new MeteredTask("document", "evaluating")
  var driver: GeolocateDocumentHadoopTaskDriver = _

  type ContextType = Mapper[Object, Text, Text, DoubleWritable]#Context

  override def setup(context: ContextType) {
    import GeolocateHadoopConfiguration._
    import GeolocateDocumentHadoopApp.progname

    val conf = context.getConfiguration()
    val ap = new ArgParser(progname)
    // Initialize set of parameters in `ap`
    new GeolocateDocumentHadoopParameters(ap)
    // Retrieve configuration values and store in `ap`
    convert_parameters_from_hadoop_conf(hadoop_conf_prefix, ap, conf)
    // Now create a class containing the stored configuration values
    val params = new GeolocateDocumentHadoopParameters(ap)
    driver = new GeolocateDocumentHadoopTaskDriver(context)
    driver.handle_parameters(params)
    driver.setup_for_run()
    evaluators =
      for ((stratname, strategy) <- driver.strategies)
        yield new ArticleGeolocateDocumentEvaluator(strategy, stratname, driver)
  }

  override def map(key: Object, value: Text, context: ContextType) {
    def process(params: Map[String, String]) {
      val table = driver.article_table
      val in_article = table.create_article(params)
      if (in_article.split == driver.params.eval_set &&
          table.would_add_article_to_list(in_article)) {
        val art = table.lookup_article(in_article.title)
        if (art == null)
          warning("Couldn't find article %s in table", in_article.title)
        else {
          for (e <- evaluators) {
            val num_processed = task.num_processed
            val doctag = "#%d" format (1 + num_processed)
            if (e.would_skip_document(art, doctag))
              errprint("Skipped article %s", art)
            else {
              // Don't put side-effecting code inside of an assert!
              val result =
                e.evaluate_document(art, doctag)
              assert(result != null)
              context.write(new Text(e.stratname),
                new DoubleWritable(result.asInstanceOf[ArticleEvaluationResult].pred_truedist))
              task.item_processed()
            }
          }
        }
      }
    }
    reader.process_row(value.toString, process _)
  }
}

class ArticleResultReducer extends
    Reducer[Text, DoubleWritable, Text, DoubleWritable] {

  type ContextType = Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context

  var driver: GeolocateDocumentHadoopTaskDriver = _

  override def setup(context: ContextType) {
    driver = new GeolocateDocumentHadoopTaskDriver(context)
  }

  override def reduce(key: Text, values: java.lang.Iterable[DoubleWritable],
      context: ContextType) {
    val errordists = (for (v <- values) yield v.get).toSeq
    val mean_dist = mean(errordists)
    val median_dist = median(errordists)
    context.write(new Text(key.toString + " mean"), new DoubleWritable(mean_dist))
    context.write(new Text(key.toString + " median"), new DoubleWritable(median_dist))
  }
}

abstract class GeolocateHadoopApp(
  progname: String
) extends GeolocateApp(progname) {
  var hadoop_conf: Configuration = _

  override type DriverType <: GeolocateHadoopJobDriver

  /* Set by subclass -- Initialize the various classes for map and reduce */
  def initialize_hadoop_classes(job: Job)

  /* Called after command-line arguments have been read, verified,
     canonicalized and stored into `arg_parser`.  We convert the arguments
     into configuration variables in the Hadoop configuration -- this is
     one way to get "side data" passed into a mapper, and is designed
     exactly for things like command-line arguments. (For big chunks of
     side data, it's better to use the Hadoop file system.) Then we
     tell Hadoop about the classes used to do map and reduce by calling
     initialize_hadoop_classes(), set the input and output files, and
     actually run the job.
   */
  override def run_program() = {
    import GeolocateHadoopConfiguration._
    convert_parameters_to_hadoop_conf(hadoop_conf_prefix, arg_parser,
      hadoop_conf)
    val job = new Job(hadoop_conf, progname)
    /* We have to call set_job() here now, and not earlier.  This is the
       "bootstrapping issue" alluded to in the comments on
       GeolocateHadoopJobDriver.  We can't set the Job until it's created,
       and we can't create the Job until after we have set the appropriate
       TextGrounder configuration parameters from the command-line arguments --
       but, we need the driver already created in order to parse the
       command-line arguments, because it participates in that process. */
    driver.set_job(job)
    initialize_hadoop_classes(job)
    for (file <- arg_holder.article_data_file)
      FileInputFormat.addInputPath(job, new Path(file))
    FileOutputFormat.setOutputPath(job, new Path(arg_holder.outfile))
    if (job.waitForCompletion(true)) 0 else 1
  }

  class GeolocateHadoopTool extends Configured with Tool {
    override def run(args: Array[String]) = {
      /* Set the Hadoop configuration object and then thread execution
         back to the ExperimentApp.  This will read command-line arguments,
         call initialize_parameters() on GeolocateApp to verify
         and canonicalize them, and then pass control back to us by
         calling run_program(), which we override. */
      hadoop_conf = getConf()
      set_errout_prefix(progname + ": ")
      implement_main(args)
    }
  }

  override def main(args: Array[String]) {
    val exitCode = ToolRunner.run(new GeolocateHadoopTool(), args)
    System.exit(exitCode)
  }
}

trait GeolocateHadoopParameters extends GeolocateDocumentParameters {
  var textgrounder_dir =
    ap.option[String]("textgrounder-dir",
      help = """Directory to use in place of TEXTGROUNDER_DIR environment
variable (e.g. in Hadoop).""")

  var outfile =
    ap.parameter[String]("outfile",
      help = """File to store evaluation results in.""")

}

class GeolocateDocumentHadoopParameters(
  parser: ArgParser = null
) extends GeolocateDocumentParameters(parser) with GeolocateHadoopParameters {
}

/**
 * Base mix-in for a Geolocate application using Hadoop.  This is a trait
 * because one of its subtraits (either GeolocateHadoopTaskDriver or
 * GeolocateHadoopJobDriver) should be mixed into a class providing the
 * implementation of an application in a way that is indifferent to whether
 * it's being run stand-alone or in Hadoop.
 */

trait GeolocateHadoopDriver extends GeolocateDriver {
  /**
   * FileHandler object for this driver.
   */
  override val file_handler = new HadoopFileHandler

  override type ArgType <: GeolocateHadoopParameters

  override def handle_parameters(args: ArgType) {
    super.handle_parameters(args)
    need(args.textgrounder_dir, "textgrounder-dir")
    TextGrounderInfo.set_textgrounder_dir(args.textgrounder_dir)
  }

  /* Implementation of the driver statistics mix-in (ExperimentDriverStats)
     that store counters in Hadoop. find_split_counter needs to be
     implemented. */

  /**
   * Find the Counter object for the given counter, split into the
   * group and tail components.  The way to do this depends on whether
   * we're running the job driver on the client, or a map or reduce task
   * on a tasktracker.
   */
  protected def find_split_counter(group: String, tail: String): Counter

  /**
   * Find the Counter object for the given counter.
   */
  protected def find_counter(name: String) = {
    val (group, counter) = split_counter(name)
      find_split_counter(group, counter)
  }

  protected def do_increment_counter(name: String, incr: Long) {
    val counter = find_counter(name)
    counter.increment(incr)
  }

  def get_counter(name: String) = {
    val counter = find_counter(name)
    counter.getValue()
  }
}

/**
 * Mix-in for the "task" (map or reduce) portion of a Geolocate application
 * using Hadoop.
 *
 * @see GeolocateHadoopDriver
 */

trait GeolocateHadoopTaskDriver extends GeolocateHadoopDriver {
  val context_object: TaskInputOutputContext[_,_,_,_]

  def find_split_counter(group: String, counter: String) = {
    context_object.getCounter(group, counter)
  }
}

/**
 * Mix-in for the "job" (main job driver) portion of a Geolocate application
 * using Hadoop.
 *
 * @see GeolocateHadoopDriver
 */

trait GeolocateHadoopJobDriver extends GeolocateHadoopDriver {
  var job: Job = _

  /**
   * Set the Job object for the driver.  Needed so we can access counters
   * on the job (typically after the job has run).  We do it this way for
   * bootstrapping reasons, rather than declaring the `job` field to be
   * abstract -- see comments in `GeolocateHadoopApp`.
   */
  def set_job(job: Job) {
    this.job = job
  }

  def find_split_counter(group: String, counter: String) = {
    job.getCounters.findCounter(group, counter)
  }
}

/**
 * Base class for running the geolocate-document app using Hadoop.
 */

abstract class GeolocateDocumentHadoopDriver extends
    GeolocateDocumentTypeDriver with GeolocateHadoopDriver {
  override type ArgType = GeolocateDocumentHadoopParameters
}

/**
 * Task driver class for running the geolocate-document app using Hadoop.
 */

class GeolocateDocumentHadoopTaskDriver(
  context: TaskInputOutputContext[_,_,_,_]
) extends GeolocateDocumentHadoopDriver with GeolocateHadoopTaskDriver {
  val context_object = context
}

/**
 * Job driver class for running the geolocate-document app using Hadoop.
 * The Job object must be set by calling `set_job`. (It's not passed in
 * as a parameter due to bootstrapping issues.  See comments in
 * `GeolocateHadoopApp`.)
 */

class GeolocateDocumentHadoopJobDriver extends
  GeolocateDocumentHadoopDriver with GeolocateHadoopJobDriver {
}

object GeolocateDocumentHadoopApp extends
    GeolocateHadoopApp("TextGrounder geolocate-document") {
  type DriverType = GeolocateDocumentHadoopJobDriver
  // FUCKING TYPE ERASURE
  def create_arg_class(ap: ArgParser) = new ArgType(ap)
  def create_driver() = new DriverType()

  def initialize_hadoop_classes(job: Job) {
    job.setJarByClass(classOf[ArticleEvaluationMapper])
    job.setMapperClass(classOf[ArticleEvaluationMapper])
    job.setReducerClass(classOf[ArticleResultReducer])
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
// trait GeolocateHadoopWritable[T] {
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
  type WritableType <: Writable
  def makeWritable(): WritableType
  def toWritable(obj: Type, w: WritableType)
  def fromWritable(w: WritableType): obj
}

object IntConverter {
  type Type = Int
  type WritableType = IntWritable
 
  def makeWritable() = new IntWritable
  def toWritable(obj: Int, w: IntWritable) { w.set(obj) }
  def fromWritable(w: WritableType) = w.get
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


object GeoArticleConverter extends RecordWriterConverter {
  type Type = GeoArticle

  def serialize(art: GeoArticle) = art.title
  def deserialize(title: String) = FIXME

  def init() {
    RecordWriterConverter.register_converter(GeoArticle, this)
  }
}


class ArticleEvaluationResultWritable extends RecordWritable {
  type Type = ArticleEvaluationResult
  def to_properties(obj: Type) =
    Seq(obj.article, obj.pred_cell, obj.true_rank,
        obj.true_cell, obj.num_arts_in_true_cell,
        obj.true_center, obj.true_truedist, obj.true_degdist,
        obj.pred_center, obj.pred_truedist, obj.pred_degdist)
  def from_properties(props: Seq[Any]) = {
    val Seq(article, pred_cell, true_rank,
        true_cell, num_arts_in_true_cell,
        true_center, true_truedist, true_degdist,
        pred_center, pred_truedist, pred_degdist) = props
    new HadoopArticleEvaluationResult(
      article.asInstanceOf[GeoArticle],
      pred_cell.asInstanceOf[GeoCell],
      true_rank.asInstanceOf[Int],
      true_cell.asInstanceOf[GeoCell],
      num_arts_in_true_cell.asInstanceOf[Int],
      true_center.asInstanceOf[Coord],
      true_truedist.asInstanceOf[Double],
      true_degdist.asInstanceOf[Double],
      pred_center.asInstanceOf[Coord],
      pred_truedist.asInstanceOf[Double],
      pred_degdist.asInstanceOf[Double]
    )
  }
}

*/
