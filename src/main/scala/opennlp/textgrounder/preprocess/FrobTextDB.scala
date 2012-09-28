///////////////////////////////////////////////////////////////////////////////
//  FrobTextDB.scala
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

import java.io._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.textdbutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.warning

import opennlp.textgrounder.gridlocate.DistDocument

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class FrobTextDBParameters(ap: ArgParser) extends
    ProcessFilesParameters(ap) {
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "DIR",
      help = """Directory containing input corpus.""")
  var input_suffix =
    ap.option[String]("s", "input-suffix",
      metavar = "DIR",
      help = """Suffix used to select the appropriate files to operate on.
Defaults to 'unigram-counts' unless --convert-to-unigram-counts is given,
in which case it defaults to 'text'.""")
  var output_suffix =
    ap.option[String]("output-suffix",
      metavar = "DIR",
      help = """Suffix used when generating the output files.  Defaults to
the value of --input-suffix, unless --convert-to-unigram-counts is given,
in which case it defaults to 'unigram-counts'.""")
  val add_field =
    ap.multiOption[String]("a", "add-field",
      metavar = "FIELD=VALUE",
      help = """Add a fixed field named FIELD, with the value VALUE.""")
  val rename_field =
    ap.multiOption[String]("rename-field",
      metavar = "FIELD=NEWFIELD",
      help = """Rename field FIELD to NEWFIELD.""")
  val remove_field =
    ap.multiOption[String]("r", "remove-field",
      metavar = "FIELD",
      help = """Remove a field, either from all rows (for a normal field)
or a fixed field.""")
  val set_split_by_value =
    ap.option[String]("set-split-by-value",
      metavar = "SPLITFIELD,MAX-TRAIN-VAL,MAX-DEV-VAL",
      help = """Set the "split" field to one of "training", "dev" or "test"
according to the value of another field (e.g. by time).  For the field named
SPLITFIELD, values <= MAX-TRAIN-VAL go into the training split;
values <= MAX-DEV-VAL go into the dev split; and higher values go into the
test split.  Comparison is lexicographically (i.e. string comparison,
rather than numeric).""")
  val split_by_field =
    ap.option[String]("split-by-field",
      metavar = "FIELD",
      help = """Divide the corpus into separate corpora according to the value
of the given field. (For example, the "split" field.)  You can combine this
action with any of the other actions, and they will be done in the right
order.""")
  val convert_to_unigram_counts =
    ap.flag("convert-to-unigram-counts",
      help = """If specified, convert the 'text' field to a 'counts' field
containing unigram counts.""")

  var split_field: String = null
  var max_training_val: String = null
  var max_dev_val: String = null
}

/**
 * A field-text file processor that outputs fields as the came in,
 * possibly modified in various ways.
 *
 * @param output_filehand FileHandler of the output corpus (directory is
 *   taken from parameters)
 * @param params Parameters retrieved from the command-line arguments
 */
class FrobTextDBFileProcessor(
  output_filehand: FileHandler,
  params: FrobTextDBParameters
) extends BasicTextDBFieldFileProcessor[Unit](params.input_suffix) {
  val split_value_to_writer = mutable.Map[String, TextDBWriter]()
  val split_value_to_outstream = mutable.Map[String, PrintStream]()
  var unsplit_writer: TextDBWriter = _
  var unsplit_outstream: PrintStream = _

  def frob_row(fieldvals: Seq[String]) = {
    val docparams = mutable.LinkedHashMap[String, String]()
    docparams ++= (rename_fields(schema.fieldnames) zip fieldvals)
    for (field <- params.remove_field)
      docparams -= field
    if (params.split_field != null) {
      if (docparams(params.split_field) <= params.max_training_val)
        docparams("split") = "training"
      else if (docparams(params.split_field) <= params.max_dev_val)
        docparams("split") = "dev"
      else
        docparams("split") = "test"
    }
    if (params.convert_to_unigram_counts) {
      val text = docparams("text")
      docparams -= "text"
      val counts = intmap[String]()
      for (word <- text.split(" ", -1))
        counts(word) += 1
      val counts_text = encode_word_count_map(counts.toSeq)
      docparams += (("counts", counts_text))
    }
    docparams.toSeq
  }

  def rename_fields(fieldnames: Seq[String]) = {
    for (field <- fieldnames) yield {
      var f = field
      for (rename_field <- params.rename_field) {
        val Array(oldfield, newfield) = rename_field.split("=", 2)
        if (f == oldfield)
          f = newfield
      }
      f
    }
  }

  def modify_fixed_values(fixed_values: Map[String, String]) = {
    var (names, values) = fixed_values.toSeq.unzip
    var new_fixed_values = (rename_fields(names) zip values).toMap
    for (field <- params.remove_field)
      new_fixed_values -= field
    val new_fields =
      for (add_field <- params.add_field) yield {
        val Array(field, value) = add_field.split("=", 2)
        (field -> value)
      }
    new_fixed_values ++= new_fields
    new_fixed_values
  }

  /**
   * Find the writer and output stream for the given frobbed document,
   * creating one or both as necessary.  There will be one writer overall,
   * and one output stream per input file.
   */
  def get_unsplit_writer_and_outstream(fieldnames: Seq[String],
      fieldvals: Seq[String]) = {
    if (unsplit_writer == null) {
      /* Construct a new schema.  Create a new writer for this schema;
         write the schema out; and record the writer in
         `unsplit_writer`.
       */
      val new_schema =
        new Schema(fieldnames, modify_fixed_values(schema.fixed_values))
      unsplit_writer = new TextDBWriter(new_schema, params.output_suffix)
      unsplit_writer.output_schema_file(output_filehand, params.output_dir,
        schema_prefix)
    }
    if (unsplit_outstream == null)
      unsplit_outstream = unsplit_writer.open_document_file(output_filehand,
        params.output_dir, current_document_prefix)
    (unsplit_writer, unsplit_outstream)
  }

  /**
   * Find the writer and output stream for the given frobbed document,
   * based on the split field, creating one or both as necessary.
   * There will be one writer for each possible split value, and one output
   * stream per split value per input file.
   */
  def get_split_writer_and_outstream(fieldnames: Seq[String],
      fieldvals: Seq[String]) = {
    val split =
      Schema.get_field_or_else(fieldnames, fieldvals, params.split_by_field)
    if (split == null)
      (null, null)
    else {
      if (!(split_value_to_writer contains split)) {
        /* Construct a new schema where the split has been moved into a
           fixed field.  Create a new writer for this schema; write the
           schema out; and record the writer in `split_value_to_writer`.
         */
        val field_map = Schema.to_map(fieldnames, fieldvals)
        field_map -= params.split_by_field
        val (new_fieldnames, new_fieldvals) = Schema.from_map(field_map)
        val new_fixed_values =
          schema.fixed_values + (params.split_by_field -> split)
        val new_schema =
          new Schema(new_fieldnames, modify_fixed_values(new_fixed_values))
        val writer = new TextDBWriter(new_schema, params.output_suffix)
        writer.output_schema_file(output_filehand, params.output_dir,
          schema_prefix + "-" + split)
        split_value_to_writer(split) = writer
      }
      if (!(split_value_to_outstream contains split)) {
        val writer = split_value_to_writer(split)
        split_value_to_outstream(split) =
          writer.open_document_file(output_filehand, params.output_dir,
            current_document_prefix + "-" + split)
      }
      (split_value_to_writer(split), split_value_to_outstream(split))
    }
  }

  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) = {
    val task = new MeteredTask("document", "frobbing")
    for (line <- lines) {
      task.item_processed()
      parse_row(line)
    }
    task.finish()
    (true, ())
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    /* Close the output stream(s), clearing the appropriate variable(s) so
       that the necessary stream(s) will be re-created again for the
       next input file.
     */
    if (params.split_by_field != null) {
      for (outstream <- split_value_to_outstream.values)
        outstream.close()
      split_value_to_outstream.clear()
    } else {
      unsplit_outstream.close()
      unsplit_outstream = null
    }
    super.end_process_file(filehand, file)
  }

  def process_row(fieldvals: Seq[String]) = {
    val (new_fieldnames, new_fieldvals) = frob_row(fieldvals).unzip
    if (params.split_by_field != null) {
      val (writer, outstream) =
        get_split_writer_and_outstream(new_fieldnames, new_fieldvals)
      if (writer == null) {
        warning("Skipped row because can't find split field: %s",
          new_fieldvals mkString "\t")
      } else {
        /* Remove the split field from the output, since it's constant
           for all rows and is moved to the fixed fields */
        val field_map = Schema.to_map(new_fieldnames, new_fieldvals)
        field_map -= params.split_by_field
        val (nosplit_fieldnames, nosplit_fieldvals) = Schema.from_map(field_map)
        assert(nosplit_fieldnames == writer.schema.fieldnames,
          "resulting fieldnames %s should be same as schema fieldnames %s"
            format (nosplit_fieldnames, writer.schema.fieldnames))
        writer.schema.output_row(outstream, nosplit_fieldvals)
      }
    } else {
      val (writer, outstream) =
        get_unsplit_writer_and_outstream(new_fieldnames, new_fieldvals)
      writer.schema.output_row(outstream, new_fieldvals)
    }
    (true, true)
  }
}

class FrobTextDBDriver extends
    ProcessFilesDriver with StandaloneExperimentDriverStats {
  type TParam = FrobTextDBParameters
  
  override def handle_parameters() {
    need(params.input_dir, "input-dir")
    if (params.set_split_by_value != null) {
      val Array(split_field, training_max, dev_max) =
        params.set_split_by_value.split(",")
      params.split_field = split_field
      params.max_training_val = training_max
      params.max_dev_val = dev_max
    }
    if (params.input_suffix == null)
      params.input_suffix =
        if (params.convert_to_unigram_counts) "text"
        else "unigram-counts"
    if (params.output_suffix == null)
      params.output_suffix =
        if (params.convert_to_unigram_counts) "unigram-counts"
        else params.input_suffix
    super.handle_parameters()
  }

  override def run_after_setup() {
    super.run_after_setup()

    val filehand = get_file_handler
    val fileproc =
      new FrobTextDBFileProcessor(filehand, params)
    fileproc.read_schema_from_textdb(filehand, params.input_dir)
    fileproc.process_files(filehand, Seq(params.input_dir))
  }
}

object FrobTextDB extends
    ExperimentDriverApp("FrobTextDB") {
  type TDriver = FrobTextDBDriver

  override def description =
"""Modify a corpus by changing particular fields.  Fields can be added
(--add-field) or removed (--remove-field); the "split" field can be
set based on the value of another field (--set-split-by-value);
the corpus can be changed from text to unigram counts
(--convert-to-unigram-counts); and it can be divided into sub-corpora
based on the value of a field, e.g. "split" (--split-by-field).
"""

  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver
}

//class ScoobiConvertTextToUnigramCountsDriver extends
//    BaseConvertTextToUnigramCountsDriver {
//
//  def usage() {
//    sys.error("""Usage: ScoobiConvertTextToUnigramCounts [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...
//
//Using Scoobi (front end to Hadoop), convert input files from raw-text format
//(one document per line) into unigram counts, in the format expected by
//TextGrounder.  OUTDIR is the directory to store the results in, which must not
//exist already.  If --group-by-user is given, a document is the concatenation
//of all tweets for a given user.  Else, each individual tweet is a document.
//""")
//  }
//
//  def process_files(filehand: FileHandler, files: Seq[String]) {
//    val task = new MeteredTask("tweet", "processing")
//    var tweet_lineno = 0
//    val task2 = new MeteredTask("user", "processing")
//    var user_lineno = 0
//    val out_counts_name = "%s/counts-only-coord-documents.txt" format (params.output_dir)
//    errprint("Counts output file is %s..." format out_counts_name)
//    val tweets = extractFromDelimitedTextFile("\t", params.files(0)) {
//      case user :: title :: text :: Nil => (user, text)
//    }
//    val counts = tweets.groupByKey.map {
//      case (user, tweets) => {
//        val counts = intmap[String]()
//        tweet_lineno += 1
//        for (tweet <- tweets) {
//          val words = tweet.split(' ')
//          for (word <- words)
//            counts(word) += 1
//        }
//        val result =
//          (for ((word, count) <- counts) yield "%s:%s" format (word, count)).
//            mkString(" ")
//        (user, result)
//      }
//    }
//
//    // Execute everything, and throw it into a directory
//    DList.persist (
//      TextOutput.toTextFile(counts, out_counts_name)
//    )
//  }
//}

//abstract class ScoobiApp(
//  progname: String
//) extends ExperimentDriverApp(progname) {
//  override def main(orig_args: Array[String]) = withHadoopArgs(orig_args) {
//    args => {
//      /* Thread execution back to the ExperimentDriverApp.  This will read
//         command-line arguments, call initialize_parameters() to verify
//         and canonicalize them, and then pass control back to us by
//         calling run_program(), which we override. */
//      set_errout_prefix(progname + ": ")
//      implement_main(args)
//    }
//  }
//}
//
//object ScoobiConvertTextToUnigramCounts extends
//    ScoobiApp("Convert raw text to unigram counts") {
//  type TDriver = ScoobiConvertTextToUnigramCountsDriver
//  def create_param_object(ap: ArgParser) = new TParam(ap)
//  def create_driver() = new TDriver
//}

