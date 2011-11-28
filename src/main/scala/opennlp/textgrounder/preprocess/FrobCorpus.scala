///////////////////////////////////////////////////////////////////////////////
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

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.experiment.ExperimentDriverApp
import opennlp.textgrounder.util.ioutil.FileHandler

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class FrobCorpusParameters(ap: ArgParser) extends
    ProcessCorpusParameters(ap) {
  val suffix =
    ap.option[String]("s", "suffix",
      default = "unigram-counts",
      metavar = "DIR",
      help = """Suffix used to select the appropriate files to operate on.
Default '%default'.""")
  val add_field =
    ap.multiOption[String]("a", "add-field",
      help = """Field to add, of the form FIELD=VAL, e.g.
'corpus=twitter-geotext-output-5-docthresh'.  Fields are added at the
beginning.""")
  val remove_field =
    ap.multiOption[String]("r", "remove-field",
      metavar = "FIELD",
      help = """Field to remove.""")
  val set_split_by_value =
    ap.multiOption[String]("set-split-by-value",
      metavar = "SPLIT-VALUE",
      help = """Set the training/dev/test splits by the value of another field
(e.g. by time).  The argument should be of the form
SPLITFIELD,MAX-TRAIN-VAL,MAX-DEV-VAL.  For the field named SPLITFIELD,
values <= MAX-TRAIN-VAL go into the training split; values <= MAX-DEV-VAL go
into the dev split; and higher values go into the test split.  Comparison is
lexicographically (i.e. string comparison, rather than numeric).""")

  var split_field: String = null
  var max_train_val: String = null
  var max_dev_val: String = null
}

/**
 * A field-text file processor that outputs fields as the came in,
 * possibly modified in various ways.
 *
 * @param input_suffix suffix used to retrieve schema and document files in
 *   in the input corpus
 * @param output_filehand FileHandler of the output corpus (directory is
 *   taken from parameters)
 * @param params Parameters retrieved from the command-line arguments
 */
class FrobCorpusFileProcessor(
  input_suffix: String, output_filehand: FileHandler,
  params: FrobCorpusParameters
) extends ProcessCorpusFileProcessor(
  input_suffix, params.suffix, output_filehand, params.output_dir
) {
  def frob_row(fieldvals: Seq[String]) = {
    val zipped_vals = (schema zip fieldvals)
    val new_fields =
      for (addfield <- params.add_field) yield {
        val Array(field, value) = addfield.split("=", 2)
        (field -> value)
      }
    val docparams = mutable.LinkedHashMap[String, String]()
    docparams ++= new_fields
    docparams ++= zipped_vals
    for (field <- params.remove_field)
      docparams -= field
    docparams.toSeq
  }
}

class FrobCorpusDriver extends ProcessCorpusDriver {
  type ParamType = FrobCorpusParameters
  
  def usage() {
    sys.error("""Usage: FrobCorpus [-i INDIR | --input-dir INDIR] [-o OUTDIR | --output-dir OUTDIR] [-s SUFFIX | --suffix SUFFIX] [-a FIELD=VALUE | --add-field FIELD_VALUE] [-r FIELD | --remove-field FIELD] ...

Modify a corpus by adding and/or removing fields.  The --add-field and
--remove-field arguments can be given multiple times.
""")
  }

  def create_file_processor(input_suffix: String) =
    new FrobCorpusFileProcessor(input_suffix, filehand, params)

  def get_input_corpus_suffix = params.suffix
}

object FrobCorpus extends
    ExperimentDriverApp("Frob a corpus, adding or removing fields") {
  type DriverType = FrobCorpusDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
