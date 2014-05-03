///////////////////////////////////////////////////////////////////////////////
//  FrobTextDB.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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

import collection.mutable
import scala.util.Random

import java.io._

import util._
import argparser._
import collection._
import error.warning
import experiment._
import io.FileHandler
import spherical._
import textdb._

import gridlocate.GridDoc

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class FrobTextDBParameters(parser: ArgParser) extends
    ProcessFilesParameters(parser) {
  val input_dir =
    ap.option[String]("i", "input-dir",
      metavar = "DIR",
      must = be_specified,
      help = """Directory containing input corpus.""")
  var input_suffix =
    ap.option[String]("s", "input-suffix",
      metavar = "DIR",
      default = "",
      help = """Suffix used to select the appropriate files to operate on.
Defaults to the empty string, which will match any TextDB file.""")
  var output_suffix =
    ap.option[String]("output-suffix",
      metavar = "DIR",
      help = """Suffix used when generating the output files.  Defaults to
the value of --input-suffix.""")
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
  val add_field_by_range =
    ap.option[String]("add-field-by-range",
      metavar = "DESTFIELD,SRCFIELD,NAME:MAXVAL,NAME:MAXVAL,...[,NAME]",
      help = """Add a field whose value is determined by the range that
another field lies within. This is normally used along with --split-by-field
to split the corpus (e.g. into training/dev/test splits) according to
the range of a given field.

For each record, each NAME:MAXVAL pair is processed in turn, and if the
value of SRCFIELD is <= MAXVAL, the DESTFIELD will be given the value NAME.
If the last entry is simply a NAME, then for all remaining records (i.e.
all values of SRCFIELD greater then the last MAXVAL given), DESTFIELD will
be assigned that NAME. Comparison is lexicographic (i.e. string comparison),
rather than numeric.""")
  val add_field_by_index =
    ap.option[String]("add-field-by-index",
      metavar = "DESTFIELD,SRCFIELD,NAME:FILE,NAME:FILE,...[,NAME]",
      help = """Add a field whose value is determined by the looking up
the value of another field in a set of index files. This is normally used
along with --split-by-field to split the corpus (e.g. into training/dev/test
splits) in a designated fashion, with the various files specifying the
particular split that each record belongs in.

For each record, each NAME:FILE pair is processed in turn, and if the
value of SRCFIELD is located in the given file, the DESTFIELD will be
given the value NAME. If the last entry is simply be a NAME, then for all
remaining records, DESTFIELD will be assigned that NAME. Each file should
have one value per line.""")
  val split_by_field =
    ap.option[String]("split-by-field",
      metavar = "FIELD",
      help = """Divide the corpus into separate corpora according to the value
of the given field. (For example, the "split" field.)  You can combine this
action with any of the other actions, and they will be done in the right
order, i.e. all field-changing operations will be done before splitting the
corpus.""")
  val convert_to_unigram_counts =
    ap.flag("convert-to-unigram-counts",
      help = """If specified, convert the data in the 'text' field to
unigram counts and add a new 'unigram-counts' field containing
those counts.""")
  val permute =
    ap.flag("permute",
      help = """Randomly permute rows of each input file.""")
  val max_rows =
    ap.option[Int]("max-rows",
      metavar = "COUNT",
      must = be_>=(0),
      help = """Write out at most COUNT rows per file.""")
  val filter_bounding_box =
    ap.option[String]("filter-bounding-box",
      metavar = "COORDS",
      help = """Filter based on the 'coord' field, only allowing those
records whose value is within a specified bounding box. The format of the
argument is four values separated by commas, i.e.

  MINLAT,MINLONG,MAXLAT,MAXLONG

For example, 25.0,-126.0,49.0,-60.0 for North America.""")

  val bounding_box =
    if (filter_bounding_box != null)
      filter_bounding_box.split(",").map(_.toDouble)
    else
      Array()
  if (bounding_box.size > 0)
    require(bounding_box.size == 4)

  val frobbers = mutable.Buffer[RecordFrobber]()

  def parse_add_field_by(value: String) = {
    val Array(destfield, srcfield, pairs@_*) = value.split(",")
    val split_pairs = pairs.map { pair =>
      pair.split(":") match {
        case Array(name,valstr) => (name, valstr)
        case Array(name) => (name, "")
      }
    }
    (destfield, srcfield, split_pairs)
  }

  if (add_field_by_range != null) {
    val (destfield, srcfield, split_pairs) =
      parse_add_field_by(add_field_by_range)
    frobbers +=
      new AddFieldByRange(destfield, srcfield, split_pairs)
  }
  if (add_field_by_index != null) {
    val (destfield, srcfield, split_pairs) =
      parse_add_field_by(add_field_by_index)
    frobbers +=
      new AddFieldByIndex(destfield, srcfield, split_pairs)
  }
  if (convert_to_unigram_counts)
    frobbers += new ConvertToUnigramCounts
  if (output_suffix == null)
    output_suffix = input_suffix
}

/**
 * Abstract class for frobbing a record in some fashion, according to
 * a command-line parameter. There will be a list of such items,
 * determined by command-line parameters, which will be processed in
 * order for each record.
 */
abstract class RecordFrobber {
  def frob(docparams: mutable.Map[String, String])
}

/**
 * Implement --add-field-by-range.
 */
class AddFieldByRange(destfield: String, srcfield: String,
    ranges: Iterable[(String, String)]) extends RecordFrobber {
  def frob(docparams: mutable.Map[String, String]) {
    val srcval = docparams(srcfield)
    for ((name, maxval) <- ranges) {
      if (maxval.isEmpty || srcval <= maxval) {
        docparams(destfield) = name
        return
      }
    }
  }
}

/**
 * Implement --add-field-by-index.
 */
class AddFieldByIndex(destfield: String, srcfield: String,
    index_files: Iterable[(String, String)]) extends RecordFrobber {
  val indices =
    for ((name, file) <- index_files) yield {
      if (file.isEmpty)
        (name, None)
      else
        (name, Some(io.localfh.openr(file).toSet))
    }
  def frob(docparams: mutable.Map[String, String]) {
    val srcval = docparams(srcfield)
    for ((name, members) <- indices) {
      if (members == None || members.get.contains(srcval)) {
        docparams(destfield) = name
        return
      }
    }
  }
}

/**
 * Implement --convert-to-unigram-counts. FIXME: Probably should be
 * deleted. Use ParseTweets.
 */
class ConvertToUnigramCounts extends RecordFrobber {
  def frob(docparams: mutable.Map[String, String]) {
    val text = docparams("text")
    val unigram_counts = intmap[String]()
    for (word <- text.split(" ", -1))
      unigram_counts(word) += 1
    val unigram_counts_text = encode_count_map(unigram_counts.toSeq)
    docparams += (("unigram-counts", unigram_counts_text))
  }
}

/**
 * A textdb processor that outputs fields as they come in, possibly modified
 * in various ways.
 *
 * @param output_filehand FileHandler of the output corpus (directory is
 *   taken from parameters)
 * @param params Parameters retrieved from the command-line arguments
 */
class FrobTextDB(
  output_filehand: FileHandler,
  params: FrobTextDBParameters
) {
  val split_value_to_writer = mutable.Map[String, TextDBWriter]()
  val split_value_to_outstream = mutable.Map[String, PrintStream]()
  var unsplit_writer: TextDBWriter = _
  var unsplit_outstream: PrintStream = _
  var schema_prefix: String = _
  var current_document_prefix: String = _

  def frob_row(schema: Schema, fieldvals: IndexedSeq[String]) = {
    val docparams = mutable.LinkedHashMap[String, String]()
    docparams ++= (rename_fields(schema.fieldnames) zip fieldvals)
    for (field <- params.remove_field)
      docparams -= field
    params.frobbers.foreach(_.frob(docparams))
    docparams.toIndexedSeq
  }

  def rename_fields(fieldnames: Iterable[String]) = {
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

  def parse_field_param(param: Seq[String]) = {
    for (fieldspec <- param) yield {
      val Array(field, value) = fieldspec.split("=", 2)
      (field -> value)
    }
  }

  def modify_fixed_values(fixed_values: scala.collection.Map[String, String]
      ) = {
    val fields_to_rename = parse_field_param(params.rename_field).toMap
    fixed_values.map {
      case (key, value) => {
        fields_to_rename.get(key) match {
          case Some(newname) => (newname, value)
          case None => (key, value)
        }
      }
    } -- params.remove_field ++ parse_field_param(params.add_field)
  }

  /**
   * Find the writer and output stream for the given frobbed document,
   * creating one or both as necessary.  There will be one writer overall,
   * and one output stream per input file.
   */
  def get_unsplit_writer_and_outstream(schema: SchemaFromFile,
      fieldnames: Iterable[String], fieldvals: IndexedSeq[String]) = {
    if (unsplit_writer == null) {
      /* Construct a new schema.  Create a new writer for this schema;
         write the schema out; and record the writer in
         `unsplit_writer`.
       */
      val new_schema =
        new Schema(fieldnames, modify_fixed_values(schema.fixed_values))
      unsplit_writer = new TextDBWriter(new_schema)
      unsplit_writer.output_schema_file(output_filehand,
        params.output_dir + "/" + schema_prefix + params.output_suffix)
    }
    if (unsplit_outstream == null)
      unsplit_outstream = unsplit_writer.open_data_file(output_filehand,
        params.output_dir + "/" + current_document_prefix +
        params.output_suffix)
    (unsplit_writer, unsplit_outstream)
  }

  /**
   * Find the writer and output stream for the given frobbed document,
   * based on the split field, creating one or both as necessary.
   * There will be one writer for each possible split value, and one output
   * stream per split value per input file.
   */
  def get_split_writer_and_outstream(schema: SchemaFromFile,
      fieldnames: Iterable[String], fieldvals: IndexedSeq[String]) = {
    new Schema(fieldnames).
      get_field_if(fieldvals, params.split_by_field) match {
        case None => (null, null)
        case Some(split) => {
          if (!(split_value_to_writer contains split)) {
            /* Construct a new schema where the split has been moved into a
               fixed field.  Create a new writer for this schema; write the
               schema out; and record the writer in `split_value_to_writer`.
             */
            val field_map = Schema.make_map(fieldnames, fieldvals)
            field_map -= params.split_by_field
            val (new_fieldnames, new_fieldvals) = Schema.unmake_map(field_map)
            val new_fixed_values =
              schema.fixed_values + (params.split_by_field -> split)
            val new_schema =
              new Schema(new_fieldnames, modify_fixed_values(new_fixed_values))
            val writer = new TextDBWriter(new_schema)
            writer.output_schema_file(output_filehand,
              params.output_dir + "/" + schema_prefix +
              "-" + split + params.output_suffix)
            split_value_to_writer(split) = writer
          }
          if (!(split_value_to_outstream contains split)) {
            val writer = split_value_to_writer(split)
            split_value_to_outstream(split) =
              writer.open_data_file(output_filehand,
                params.output_dir + "/" + current_document_prefix +
                "-" + split + params.output_suffix)
          }
          (split_value_to_writer(split), split_value_to_outstream(split))
        }
      }
  }

  def filter_row(schema: SchemaFromFile, fieldvals: IndexedSeq[String]) = {
    if (params.bounding_box.size == 0)
      true
    else {
      val Array(minlat, minlong, maxlat, maxlong) = params.bounding_box
      schema.get_value_if[SphereCoord](fieldvals, "coord") match {
        case None => false
        case Some(coord) =>
          coord.lat >= minlat && coord.lat <= maxlat &&
          coord.long >= minlong && coord.long <= maxlong
      }
    }
  }

  def process_row(schema: SchemaFromFile, fieldvals: IndexedSeq[String]) {
    val (new_fieldnames, new_fieldvals) = frob_row(schema, fieldvals).unzip
    if (filter_row(schema, fieldvals)) {
      if (params.split_by_field != null) {
        val (writer, outstream) =
          get_split_writer_and_outstream(schema, new_fieldnames, new_fieldvals)
        if (writer == null) {
          warning("Skipped row because can't find split field: %s",
            new_fieldvals mkString "\t")
        } else {
          /* Remove the split field from the output, since it's constant
             for all rows and is moved to the fixed fields */
          val field_map = Schema.make_map(new_fieldnames, new_fieldvals)
          field_map -= params.split_by_field
          val (nosplit_fieldnames, nosplit_fieldvals) = Schema.unmake_map(field_map)
          assert(nosplit_fieldnames == writer.schema.fieldnames,
            "resulting fieldnames %s should be same as schema fieldnames %s"
              format (nosplit_fieldnames, writer.schema.fieldnames))
          outstream.println(writer.schema.make_line(nosplit_fieldvals))
        }
      } else {
        val (writer, outstream) =
          get_unsplit_writer_and_outstream(schema, new_fieldnames, new_fieldvals)
        outstream.println(writer.schema.make_line(new_fieldvals))
      }
    }
  }

  def process_dir(filehand: FileHandler, dir: String) {
    val (schema, files) =
      TextDB.get_textdb_files(filehand, dir, suffix_re = params.input_suffix)
    val (_, schpref, _, _) = Schema.split_schema_file(filehand,
      schema.filename, params.input_suffix).get
    schema_prefix = schpref

    for (file <- files) {
      val (_, prefix, _, _) = TextDB.split_data_file(
        filehand, file, params.input_suffix).get
      current_document_prefix = prefix
      val rows1 = TextDB.read_textdb_file(filehand, file, schema)
      val rows2 =
        if (params.permute) (new Random()).shuffle(rows1.toSeq).toIterator
        else rows1
      val rows =
        if (params.max_rows > 0) rows2.take(params.max_rows)
        else rows2
      for (fieldvals <- rows)
        process_row(schema, fieldvals)
      /* Close the output stream(s), clearing the appropriate variable(s)
         so that the necessary stream(s) will be re-created again for the
         next input file.

         FIXME: This is rather bogus, since we directly know now when we're
         opening a new file. (It was written this way using an internal
         iterator -- the no-longer-existing TextDB and
         FileProcessor interface.)
       */
      if (params.split_by_field != null) {
        for (outstream <- split_value_to_outstream.values)
          outstream.close()
        split_value_to_outstream.clear()
      } else {
        unsplit_outstream.close()
        unsplit_outstream = null
      }
    }
  }
}

class FrobTextDBDriver extends
    ProcessFilesDriver with StandaloneExperimentDriverStats {
  type TParam = FrobTextDBParameters

  override def run() {
    super.run()

    val filehand = get_file_handler
    val fileproc = new FrobTextDB(filehand, params)
    fileproc.process_dir(filehand, params.input_dir)
  }
}

object FrobTextDB extends ExperimentDriverApp("FrobTextDB") {
  type TDriver = FrobTextDBDriver

  override def description =
"""Modify a corpus by changing particular fields.  Fields can be added
(--add-field) or removed (--remove-field); the "split" field can be
set based on the value of another field (--set-split-by-value);
unigram counts can be generated from text (--convert-to-unigram-counts);
and it can be divided into sub-corpora based on the value of a field,
e.g. "split" (--split-by-field).
"""

  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}
