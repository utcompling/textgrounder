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

package opennlp.textgrounder.geolocate

import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.ioutil.{errprint, warning, FileHandler}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.textutil.splittext

import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                               //
/////////////////////////////////////////////////////////////////////////////

class GeoDocumentWriter(outfile: PrintStream, outfields: Seq[String]) {
  def output_header() {
    outfile.println(outfields mkString "\t")
  }
  
  def output_row(doc: GeoDocument) {
    outfile.println(doc.get_fields(outfields) mkString "\t")    
  }
}

class GeoDocumentReader(infields: Seq[String]) {
  var num_processed = 0
  def process_row(line: String, process: Map[String,String] => Unit,
      line_number: Int = -1) {
    val lineno = if (line_number < 0) num_processed + 1 else line_number
    // println("[%s]" format line)
    val fieldvals = splittext(line, '\t')
    if (fieldvals.length != infields.length)
      warning(
      """Strange record at line #%s, expected %s fields, saw %s fields;
      skipping line=%s""", lineno, infields.length, fieldvals.length, line)
    else {
      var good = true
      for ((field, value) <- infields zip fieldvals) {
        if (!GeoDocument.validate_field(field, value)) {
          good = false
          warning(
      """Bad field value at line #%s, field=%s, value=%s,
      skipping line=%s""", lineno, field, value, line)
        }
      }
      if (good)
        process((infields zip fieldvals).toMap)
    }
    num_processed += 1
  }
}

object GeoDocumentData {
  val combined_document_data_outfields = List("id", "title", "split", "redir",
      "namespace", "is_list_of", "is_disambig", "is_list", "coord",
      "incoming_links")

  /** Read in the document data file.  Call `process` on each document.
   *
   * @param filehand File handler for the file.
   * @param filename Name of the file to read in.
   * @param process Function to be called for each document.
   * @param A value in seconds, which limits the total processing time
   *   (real time, not CPU time) used for reading in the file, for
   *   testing purposes.
   */
  def read_document_file(filehand: FileHandler, filename: String,
      process: Map[String,String] => Unit, maxtime: Double=0.0) = {
    errprint("Reading document data from %s...", filename)
    val task = new MeteredTask("document", "reading")

    val fi = filehand.openr(filename)

    val fields = splittext(fi.next(), '\t')
    val reader = new GeoDocumentReader(fields)
    breakable {
      for (line <- fi) {
        // If we've processed no documents so far, we're on line 2
        // because line 1 is the header.
        reader.process_row(line, process, task.num_processed + 2)
        if (task.item_processed(maxtime=maxtime))
          break
      }
    }
    task.finish()
    output_resource_usage()
    fields
  }

  def write_document_file(outfile: PrintStream, outfields: Seq[String],
      documents: Iterable[GeoDocument]) {
    val writer = new GeoDocumentWriter(outfile, outfields)
    writer.output_header()
    for (doc <- documents)
      writer.output_row(doc)
    outfile.close()
  }
}

/** A document for the purpose of geolocation.  The fields available
 * depend on the source of the document (e.g. Wikipedia, etc.).
 *
 * Defined fields:
 *
 * title: Title of document.
 * id: ID of document, as an int.
 * coord: Coordinates of document.
 * incoming_links: Number of incoming links, or None if unknown.
 * split: Split of document ("training", "dev", "test")
 * redir: If this is a redirect, document title that it redirects to; else
 *          an empty string.
 * namespace: Namespace of document (e.g. "Main", "Wikipedia", "File")
 * is_list_of: Whether document title is "List of *"
 * is_disambig: Whether document is a disambiguation page.
 * is_list: Whether document is a list of any type ("List of *", disambig,
 *          or in Category or Book namespaces)
 */
class GeoDocument(params: Map[String,String]) {
  var title="unknown"
  var id=0
  var coord: Coord=null
  var incoming_links: Option[Int]=None
  var split="unknown"
  var redir=""
  var namespace="Main"
  var is_list_of=false
  var is_disambig=false
  var is_list=false
  import GeoDocument._, GeoDocumentConverters._

  for ((name, v) <- params) {
    name match {
      case "id" => id = v.toInt
      case "title" => title = v
      case "split" => split = v
      case "redir" => redir = v
      case "namespace" => namespace = v
      case "is_list_of" => is_list_of = yesno_to_boolean(v)
      case "is_disambig" => is_disambig = yesno_to_boolean(v)
      case "is_list" => is_list = yesno_to_boolean(v)
      case "coord" => coord = commaval_to_coord(v)
      case "incoming_links" => incoming_links = get_int_or_blank(v)
      }
  }

  def get_fields(fields: Traversable[String]) = {
    for (field <- fields) yield {
      field match {
        case "id" => id.toString
        case "title" => title
        case "split" => split
        case "redir" => redir
        case "namespace" => namespace
        case "is_list_of" => boolean_to_yesno(is_list_of)
        case "is_disambig" => boolean_to_yesno(is_disambig)
        case "is_list" => boolean_to_yesno(is_list)
        case "coord" => coord_to_commaval(coord)
        case "incoming_links" => put_int_or_blank(incoming_links)
      }
    }
  }

  override def toString() = {
    val coordstr = if (coord != null) " at %s".format(coord) else ""
    val redirstr =
      if (redir.length > 0) ", redirect to %s".format(redir) else ""
    "%s(%s)%s%s".format(title, id, coordstr, redirstr)
  }

 def adjusted_incoming_links = adjust_incoming_links(incoming_links)
}

object GeoDocument {
  /**
   * Compute the short form of a document name.  If short form includes a
   * division (e.g. "Tucson, Arizona"), return a tuple (SHORTFORM, DIVISION);
   * else return a tuple (SHORTFORM, None).
   */
  def compute_short_form(name: String) = {
    val includes_div_re = """(.*?), (.*)$""".r
    val includes_parentag_re = """(.*) \(.*\)$""".r
    name match {
      case includes_div_re(tucson, arizona) => (tucson, arizona)
      case includes_parentag_re(tucson, city) => (tucson, null)
      case _ => (name, null)
    }
  }

  def log_adjust_incoming_links(links: Int) = {
    if (links == 0) // Whether from unknown count or count is actually zero
      0.01 // So we don't get errors from log(0)
    else links
  }

  def adjust_incoming_links(incoming_links: Option[Int]) = {
    val ail =
      incoming_links match {
        case None => {
          if (debug("some"))
            warning("Strange, object has no link count")
          0
        }
        case Some(il) => {
          if (debug("some"))
            errprint("--> Link count is %s", il)
          il
        }
      }
    ail
  }

  def validate_field(field: String, value: String) = {
    import GeoDocumentConverters._
    field match {
      case "id" => validate_int(value)
      case "title" => true
      case "split" => true
      case "redir" => true
      case "namespace" => true
      case "is_list_of" => validate_boolean(value)
      case "is_disambig" => validate_boolean(value)
      case "is_list" => validate_boolean(value)
      case "coord" => validate_coord(value)
      case "incoming_links" => validate_int_or_blank(value)
      case _ => false
    }
  }
}

/************************ Conversion functions ************************/
object GeoDocumentConverters {
  def yesno_to_boolean(foo: String)  = {
    foo match {
      case "yes" => true
      case "no" => false
      case _ => {
        warning("Expected yes or no, saw '%s'", foo)
        false
      }
    }
  }
  
  def boolean_to_yesno(foo: Boolean) = if (foo) "yes" else "no"

  def validate_boolean(foo: String) = foo == "yes" || foo == "no"
  
  def commaval_to_coord(foo: String) = {
    if (foo != "") {
      val Array(lat, long) = foo.split(',')
      Coord(lat.toDouble, long.toDouble)
    } else null
  }
  
  def coord_to_commaval(foo: Coord) =
    if (foo != null) "%s,%s".format(foo.lat, foo.long) else ""

  def validate_coord(foo: String): Boolean = {
    if (foo == "") return true
    val split = splittext(foo, ',')
    if (split.length != 2) return false
    val Array(lat, long) = split
    try {
      Coord(lat.toDouble, long.toDouble)
    } catch {
      case _ => return false
    }
    return true
  }
  
  def get_int_or_blank(foo: String) =
    if (foo == "") None else Option[Int](foo.toInt)
 
  def put_int_or_blank(foo: Option[Int]) = {
    foo match {
      case None => ""
      case Some(x) => x.toString
    }
  }

  def validate_int(foo: String) = {
    try {
      foo.toInt
      true
    } catch {
      case _ => false
    }
  }

  def validate_int_or_blank(foo: String) = {
    if (foo == "") true
    else validate_int(foo)
  }
}
