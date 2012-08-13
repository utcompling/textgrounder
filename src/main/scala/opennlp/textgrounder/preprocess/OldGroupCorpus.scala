///////////////////////////////////////////////////////////////////////////////
//  GroupCorpus.scala
//
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

import collection.mutable
import collection.JavaConversions._

import java.io._

import org.apache.hadoop.io._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.corpusutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.hadoop._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.mathutil.mean
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.warning

import opennlp.textgrounder.gridlocate.DistDocument

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////
/*

We have a corpus consisting of a single directory and suffix identifying
the corpus.  In the corpus is a schema file plus one or more document
files.  We want to group all documents together that have the same value
for a particular field, usually the 'user' field (i.e. group together
all documents with the same user).

NOTE: Do we want to do this before or after splitting on train/test/dev?
If we do this after, we end up with up to three documents per user -- one
per each split.  Because of the way we chose the splits, they will be
offset in time.  This could definitely be a useful test.  If we do this
before, we end up with only one document per user, and then have to split
the users, perhaps randomly, perhaps by (average?) time as well.

Note that no changes are needed in the coding of this program to make it work
regardless of whether we end up splitting before or after grouping.
When we split before grouping, we run the grouping program separately on
each subcorpus; when we split after, we run on the whole corpus.

What about other fields?  The other fields in the Infochimps corpus are as
follows, with a description of what to do in each case.

1. 'id' = Tweet ID: Throw it away.
2. 'coord' = Coordinate: Take some sort of average.  It might be enough
    to simply average the latitude and longitudes.  But we need to be careful
    averaging longitudes, because they are cyclic (wrap around); e.g. +179
    longitude and -177 longitude are only 4 degrees apart in longitude; hence
    the average should obviously be -179.  However, a simple-minded averaging
    produces +1 longitude, which is halfway around the world from either of
    the original values (and exactly 180 degrees away from the correct value).
    The standard rule in this case is to take advantage of modular arithmetic
    to place the two values as close as possible -- which in all cases will
    be no more than 180 degrees -- then average, and finally readjust to be
    within the preferred range.  In the example above, we could average
    correctly by adding 360 to -177 to become +183; then the difference
    between the the two values is only 4.  The average is +181, which can be
    readjusted to fall within range by subtracting 360, resulting in -179.
    When there are more than two points, it's necessary to choose the
    representation of the points that minimizes the distance between the
    minimum and maximum.  We probably also want to use some sort of outlier
    detection mechanism and reject the outliers entirely, so they don't skew
    the results; or choose the median instead of mean.  The correct algorithms
    for all this might be tricky, but are critical because the coordinate is
    the basis of geolocation.  Surely there must already exist algorithms to
    do this in an efficient and robust manner, and surely there must already
    exist implementations of these algorithms.  For example, what did
    Eisenstein et al. (2010) do to produce their GeoText corpus?  They surely
    struggled with this issue.

    Note that it seems that, in the majority of cases where all the values
    are fairly close together, the maximum distance between points should be
    less than 180 degrees.  If so, we can number the points using both a
    [-180,+180) scale and a [0, 360) scale, sort in each case and see what
    the difference between minimum and maximum is.  Choose the smaller of
    the differences, and if it's <= 180, we know this is the correct scale,
    and we can average using this scale.  If both are > 180, it might work
    as well in all cases to select the smaller one, but I'm not sure; that
    would have to be proven.

3. 'time' = Time: Either throw away, or produce an average, standard deviation,
    min and max.
4. 'username' = There will be only one.
5. 'userid' = There may be more than one; if so, list any that get at least 25%
   of the total, in order starting from the most common (break ties randomly).
   Include 'other' as an additional possibility if all non-included values
   together include at least 25% of the total, and sort 'other' along with the
   others according to its percent.
6. 'reply_username', 'reply_userid' = Same as for 'userid'
7. 'anchor' = Same as for 'userid'
8. 'lang' = Same as for 'userid'
9. 'counts' = Combine counts according to keys, adding the values of cases
    where the same key occurs in both records.
10. 'text' = Concatenate the text of the Tweets.  It's probably good to
    put an -EOT- ("end of Tweet") token after each Tweet so that they can
    still be distinguished when concatenated together.

So, we receive in the mapper single documents.  We extract the group and
the remaining fields and output a pair where the key is the group (typically,
the user name), and the value is the remaining fields.  The reducer then needs
to reduce the whole set of records down to a single one using the instructions
above, and output a single combined text record.  Possibly some of the work
could also be done in a combiner, but it's probably not worth the extra effort
to construct such a combiner, since we'd have to complexify the intermediate
format away from text to some kind of binary format.

*/

class GroupCorpusParameters(ap: ArgParser) extends
    ArgParserParameters(ap) {
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "DIR",
      help = """Directory containing input corpus.""")
  var input_suffix =
    ap.option[String]("s", "input-suffix",
      metavar = "DIR",
      default = "unigram-counts",
      help = """Suffix used to select the appropriate files to operate on.
Defaults to '%default'.""")
  var output_suffix =
    ap.option[String]("output-suffix",
      metavar = "DIR",
      help = """Suffix used when generating the output files.  Defaults to
the value of --input-suffix.""")
  val output_dir =
    ap.positional[String]("output-dir",
      help = """Directory to store output files in.  It must not already
exist, and will be created (including any parent directories).""")
  val field =
    ap.option[String]("f", "field",
      default = "username",
      help = """Field to group on; default '%default'.""")
}

class GroupCorpusDriver extends
    HadoopableArgParserExperimentDriver with HadoopExperimentDriver {
  type TParam = GroupCorpusParameters
  type TRunRes = Unit
  
  def handle_parameters() {
    need(params.input_dir, "input-dir")
    if (params.output_suffix == null)
      params.output_suffix = params.input_suffix
  }
  def setup_for_run() { }

  def run_after_setup() { }
}

/**
 * This file processor, the GroupCorpusMapReducer trait below and the
 * subclass of this file processor together are a lot of work simply to read
 * the schema from the input corpus in both the mapper and reducer.  Perhaps
 * we could save the schema and pass it to the reducer as the first item?
 * Perhaps that might not make the code any simpler.
 */
class GroupCorpusFileProcessor(
  context: TaskInputOutputContext[_,_,_,_],
  driver: GroupCorpusDriver
) extends CorpusFileProcessor(driver.params.input_suffix) {
  def process_row(fieldvals: Seq[String]): (Boolean, Boolean) =
    throw new IllegalStateException("This shouldn't be called")

  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) =
    throw new IllegalStateException("This shouldn't be called")
}

trait GroupCorpusMapReducer extends HadoopExperimentMapReducer {
  def progname = GroupCorpus.progname
  type TDriver = GroupCorpusDriver
  // more type erasure crap
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver
  def create_processor(context: TContext) =
    new GroupCorpusFileProcessor(context, driver)

  var processor: GroupCorpusFileProcessor = _
  override def init(context: TContext) {
    super.init(context)
    processor = create_processor(context)
    processor.read_schema_from_corpus(driver.get_file_handler,
        driver.params.input_dir)
    context.progress
  }
}

class GroupCorpusMapper extends
    Mapper[Object, Text, Text, Text] with GroupCorpusMapReducer {
  type TContext = Mapper[Object, Text, Text, Text]#Context

  class GroupCorpusMapFileProcessor(
    map_context: TContext,
    driver: GroupCorpusDriver
  ) extends GroupCorpusFileProcessor(map_context, driver) {
    override def process_row(fieldvals: Seq[String]) = {
      map_context.write(
        new Text(schema.get_field(fieldvals, driver.params.field)),
        new Text(fieldvals.mkString("\t")))
      (true, true)
    }
  }

  override def create_processor(context: TContext) =
    new GroupCorpusMapFileProcessor(context, driver)

  override def setup(context: TContext) { init(context) }

  override def map(key: Object, value: Text, context: TContext) {
    processor.parse_row(value.toString)
    context.progress
  }
}

class GroupCorpusReducer extends
    Reducer[Text, Text, Text, NullWritable] with GroupCorpusMapReducer {
  type TContext = Reducer[Text, Text, Text, NullWritable]#Context

  def average_coords(coords: Iterable[(Double, Double)]) = {
    val (lats, longs) = coords.unzip
    // FIXME: Insufficiently correct!
    "%s,%s" format (mean(lats.toSeq), mean(longs.toSeq))
  }

  def compute_most_common(values: mutable.Buffer[String]) = {
    val minimum_percent = 0.25
    val len = values.length
    val counts = intmap[String]()
    for (v <- values)
      counts(v) += 1
    val other = counts.values.filter(_.toDouble/len < minimum_percent).sum
    counts("other") = other
    val sorted = counts.toSeq.sortWith(_._2 > _._2)
    (for ((v, count) <- sorted if count.toDouble/len >= minimum_percent)
      yield v).mkString(",")
  }

  def combine_text(values: mutable.Buffer[String]) = {
    values.mkString(" EOT ") + " EOT "
  }

  def combine_counts(values: mutable.Buffer[String]) = {
    val counts = intmap[String]()
    val split_last_colon = "(.*):([^:]*)".r
    for (vv <- values; v <- vv.split(" ")) {
      v match {
        case split_last_colon(word, count) => counts(word) += count.toInt
        case _ =>
          warning("Saw bad item in counts field, without a colon in it: %s",
            v)
      }
    }
    (for ((word, count) <- counts) yield "%s:%s" format (word, count)).
      mkString(" ")
  }

  override def setup(context: TContext) { init(context) }

  override def reduce(key: Text, values: java.lang.Iterable[Text],
      context: TContext) {
    var num_tweets = 0
    val coords = mutable.Buffer[(Double, Double)]()
    val times = mutable.Buffer[Long]()
    val userids = mutable.Buffer[String]()
    val reply_usernames = mutable.Buffer[String]()
    val reply_userids = mutable.Buffer[String]()
    val anchors = mutable.Buffer[String]()
    val langs = mutable.Buffer[String]()
    val countses = mutable.Buffer[String]()
    val texts = mutable.Buffer[String]()
    for (vv <- values) {
      num_tweets += 1
      val fieldvals = vv.toString.split("\t")
      for (v <- processor.schema.fieldnames zip fieldvals) {
        v match {
          case ("coord", coord) => {
            val Array(lat, long) = coord.split(",")
            coords += ((lat.toDouble, long.toDouble))
          }
          case ("time", time) => times += time.toLong
          case ("userid", userid) => userids += userid
          case ("reply_username", reply_username) =>
            reply_usernames += reply_username
          case ("reply_userid", reply_userid) =>
            reply_userids += reply_userid
          case ("anchor", anchor) => anchors += anchor
          case ("lang", lang) => langs += lang
          case ("counts", counts) => countses += counts
          case ("text", text) => texts += text
          case (_, _) => { }
        }
      }
    }
    val output = mutable.Buffer[(String, String)]()
    for (field <- processor.schema.fieldnames) {
      field match {
        case "coord" => output += (("coord", average_coords(coords)))
        case "time" => {
          output += (("mintime", times.min.toString))
          output += (("maxtime", times.max.toString))
          output +=
            (("avgtime", mean(times.map(_.toDouble).toSeq).toLong.toString))
        }
        case "userid" => output += (("userid", compute_most_common(userids)))
        case "reply_userid" =>
          output += (("reply_userid", compute_most_common(reply_userids)))
        case "reply_username" =>
          output += (("reply_username", compute_most_common(reply_usernames)))
        case "anchor" => output += (("anchor", compute_most_common(anchors)))
        case "lang" => output += (("lang", compute_most_common(langs)))
        case "counts" => output += (("counts", combine_counts(countses)))
        case "text" => output += (("text", combine_text(texts)))
        case _ => { }
      }
    }
    val (outkeys, outvalues) = output.unzip
    context.write(new Text("%s\t%s\t%s" format (key.toString, num_tweets,
      outvalues.mkString("\t"))), null)
  }
}

object GroupCorpus extends
    ExperimentDriverApp("GroupCorpus") with HadoopCorpusApp {
  type TDriver = GroupCorpusDriver

  override def description =
"""Group rows in a corpus according to the value of a field (e.g. the "user"
field).  The "text" and "counts" fields are combined appropriately.
"""

  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()

  def corpus_suffix = driver.params.input_suffix
  def corpus_dirs = Seq(driver.params.input_dir)

  override def initialize_hadoop_input(job: Job) {
    super.initialize_hadoop_input(job)
    FileOutputFormat.setOutputPath(job, new Path(driver.params.output_dir))
  }

  def initialize_hadoop_classes(job: Job) {
    job.setJarByClass(classOf[GroupCorpusMapper])
    job.setMapperClass(classOf[GroupCorpusMapper])
    job.setReducerClass(classOf[GroupCorpusReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[Text])
  }
}

