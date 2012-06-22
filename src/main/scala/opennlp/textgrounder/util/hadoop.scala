///////////////////////////////////////////////////////////////////////////////
//  hadoop.scala
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

package opennlp.textgrounder.util

import org.apache.hadoop.io._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs._

// The following says to import everything except java.io.FileSystem, because
// it conflicts with Hadoop's FileSystem. (Technically, it imports everything
// but in the process aliases FileSystem to _, which has the effect of making
// it inaccessible. _ is special in Scala and has various meanings.)
import java.io.{FileSystem=>_,_}
import java.net.URI

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.printutil.{errprint, set_errout_prefix}

package object hadoop {
  class HadoopFileHandler(conf: Configuration) extends FileHandler {
    protected def get_file_system(filename: String) = {
      FileSystem.get(URI.create(filename), conf)
    }

    def get_raw_input_stream(filename: String) =
      get_file_system(filename).open(new Path(filename))
  
    def get_raw_output_stream(filename: String) =
      get_file_system(filename).create(new Path(filename))

    def split_filename(filename: String) = {
      val path = new Path(filename)
      (path.getParent.toString, path.getName)
    }

    def join_filename(dir: String, file: String) =
      new Path(dir, file).toString

    def is_directory(filename: String) =
      get_file_system(filename).getFileStatus(new Path(filename)).isDir

    def make_directories(filename: String):Boolean =
      get_file_system(filename).mkdirs(new Path(filename))

    def list_files(dir: String) = {
      for (file <- get_file_system(dir).listStatus(new Path(dir)))
        yield file.getPath.toString
    }
  }

  object HadoopExperimentConfiguration {
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

  trait HadoopExperimentDriverApp extends ExperimentDriverApp {
    var hadoop_conf: Configuration = _

    override type TDriver <: HadoopExperimentDriver

    /* Set by subclass -- Initialize the various classes for map and reduce */
    def initialize_hadoop_classes(job: Job)

    /* Set by subclass -- Set the settings for reading appropriate input files,
       possibly based on command line arguments */
    def initialize_hadoop_input(job: Job)

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
      import HadoopExperimentConfiguration._
      convert_parameters_to_hadoop_conf(hadoop_conf_prefix, arg_parser,
        hadoop_conf)
      val job = new Job(hadoop_conf, progname)
      /* We have to call set_job() here now, and not earlier.  This is the
         "bootstrapping issue" alluded to in the comments on
         HadoopExperimentDriver.  We can't set the Job until it's created,
         and we can't create the Job until after we have set the appropriate
         TextGrounder configuration parameters from the command-line arguments --
         but, we need the driver already created in order to parse the
         command-line arguments, because it participates in that process. */
      driver.set_job(job)
      initialize_hadoop_classes(job)
      initialize_hadoop_input(job)

      if (job.waitForCompletion(true)) 0 else 1
    }

    class HadoopExperimentTool extends Configured with Tool {
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
      val exitCode = ToolRunner.run(new HadoopExperimentTool(), args)
      System.exit(exitCode)
    }
  }

  trait HadoopCorpusApp extends HadoopExperimentDriverApp {
    def corpus_suffix: String

    def corpus_dirs: Iterable[String]

    def initialize_hadoop_input(job: Job) {
      /* A very simple file processor that does nothing but note the files
         seen, for Hadoop's benefit. */
      class RetrieveDocumentFilesFileProcessor(
        suffix: String
      ) extends CorpusFileProcessor(suffix) {
        def process_lines(lines: Iterator[String],
            filehand: FileHandler, file: String,
            compression: String, realname: String) = {
          errprint("Called with %s", file)
          FileInputFormat.addInputPath(job, new Path(file))
          true
        }

        def process_row(fieldvals: Seq[String]) =
          throw new IllegalStateException("This shouldn't be called")
      }

      val fileproc = new RetrieveDocumentFilesFileProcessor(
        // driver.params.eval_set + "-" + driver.document_file_suffix
        corpus_suffix
      )
      fileproc.process_files(driver.get_file_handler, corpus_dirs)
      // FileOutputFormat.setOutputPath(job, new Path(params.outfile))
    }
  }

  /**
   * Base mix-in for an Experiment application using Hadoop.
   *
   * @see HadoopExperimentDriver
   */

  trait BaseHadoopExperimentDriver extends
    HadoopableArgParserExperimentDriver {
    /**
     * FileHandler object for this driver.
     */
    private lazy val hadoop_file_handler =
      new HadoopFileHandler(get_configuration)

    override def get_file_handler: FileHandler = hadoop_file_handler

    // override type TParam <: HadoopExperimentParameters

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

    def get_job_context: JobContext

    def get_configuration = get_job_context.getConfiguration

    def get_task_id = get_configuration.getInt("mapred.task.partition", -1)
    
    /**
     * Find the Counter object for the given counter.
     */
    protected def find_counter(name: String) = {
      val (group, counter) = split_counter(name)
        find_split_counter(group, counter)
    }

    protected def imp_increment_counter(name: String, incr: Long) {
      val counter = find_counter(name)
      counter.increment(incr)
    }

    protected def imp_get_counter(name: String) = {
      val counter = find_counter(name)
      counter.getValue()
    }
  }

  /**
   * Mix-in for an Experiment application using Hadoop.  This is a trait
   * because it should be mixed into a class providing the implementation of
   * an application in a way that is indifferent to whether it's being run
   * stand-alone or in Hadoop.
   *
   * This is used both in map/reduce task code and in the client job-running
   * code.  In some ways it would be cleaner to have separate classes for
   * task vs. client job code, but that would entail additional boilerplate
   * for any individual apps as they'd have to create separate task and
   * client job versions of each class along with a base superclass for the
   * two.
   */

  trait HadoopExperimentDriver extends BaseHadoopExperimentDriver {
    var job: Job = _
    var context: TaskInputOutputContext[_,_,_,_] = _

    /**
     * Set the task context, if we're running in the map or reduce task
     * code on a tasktracker. (Both Mapper.Context and Reducer.Context are
     * subclasses of TaskInputOutputContext.)
     */
    def set_task_context(context: TaskInputOutputContext[_,_,_,_]) {
      this.context = context
    }

    /**
     * Set the Job object, if we're running the job-running code on the
     * client. (Note that we have to set the job like this, rather than have
     * it passed in at creation time, e.g. through an abstract field,
     * because of bootstrapping issues; explained in HadoopExperimentApp.
     */

    def set_job(job: Job) {
      this.job = job
    }

    def get_job_context = {
      if (context != null) context
      else if (job != null) job
      else need_to_set_context()
    }

    def find_split_counter(group: String, counter: String) = {
      if (context != null)
        context.getCounter(group, counter)
      else if (job != null)
        job.getCounters.findCounter(group, counter)
      else
        need_to_set_context()
    }

    def need_to_set_context() =
      throw new IllegalStateException("Either task context or job needs to be set before any counter operations")

    override def heartbeat() {
      if (context != null)
        context.progress
    }
  }

  trait HadoopExperimentMapReducer {
    type TContext <: TaskInputOutputContext[_,_,_,_]
    type TDriver <: HadoopExperimentDriver
    val driver = create_driver()
    type TParam = driver.TParam

    def progname: String

    def create_param_object(ap: ArgParser): TParam
    def create_driver(): TDriver

    /** Originally this was simply called 'setup', but used only for a
     * trait that could be mixed into a mapper.  Expanding this to allow
     * it to be mixed into both a mapper and a reducer didn't cause problems
     * but creating a subtrait that override this function did cause problems,
     * a complicated message like this:

[error] /Users/benwing/devel/textgrounder/src/main/scala/opennlp/textgrounder/preprocess/GroupCorpus.scala:208: overriding method setup in class Mapper of type (x$1: org.apache.hadoop.mapreduce.Mapper[java.lang.Object,org.apache.hadoop.io.Text,org.apache.hadoop.io.Text,org.apache.hadoop.io.Text]#Context)Unit;
[error]  method setup in trait GroupCorpusMapReducer of type (context: GroupCorpusMapper.this.TContext)Unit cannot override a concrete member without a third member that's overridden by both (this rule is designed to prevent ``accidental overrides'')
[error] class GroupCorpusMapper extends
[error]       ^

     * so I got around it by defining the actual code in another method and
     * making the setup() calls everywhere call this. (FIXME unfortunately this
     * is error-prone.)
     */
    def init(context: TContext) {
      import HadoopExperimentConfiguration._

      val conf = context.getConfiguration
      val ap = new ArgParser(progname)
      // Initialize set of parameters in `ap`
      create_param_object(ap)
      // Retrieve configuration values and store in `ap`
      convert_parameters_from_hadoop_conf(hadoop_conf_prefix, ap, conf)
      // Now create a class containing the stored configuration values
      val params = create_param_object(ap)
      driver.set_task_context(context)
      context.progress
      driver.set_parameters(params)
      context.progress
      driver.setup_for_run()
      context.progress
    }
  }
}
