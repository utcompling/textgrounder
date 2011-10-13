package opennlp.textgrounder.geolocate

import NlpUtil._
import Distances._
import util.control.Breaks._
import java.io._

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                               //
/////////////////////////////////////////////////////////////////////////////

class ArticleWriter(outfile: PrintStream, outfields: Seq[String]) {
  def output_header() {
    outfile.println(outfields mkString "\t")
  }
  
  def output_row(art: Article) {
    outfile.println(art.get_fields(outfields) mkString "\t")    
  }
}

object ArticleData {
  val combined_article_data_outfields = List("id", "title", "split", "redir",
      "namespace", "is_list_of", "is_disambig", "is_list", "coord",
      "incoming_links")

  // Read in the article data file.  Call PROCESS on each article.
  // The type of the article created is given by ARTICLE_TYPE, which defaults
  // to Article.  MAXTIME is a value in seconds, which limits the total
  // processing time (real time, not CPU time) used for reading in the
  // file, for testing purposes.
  def read_article_data_file(filename: String,
      process: Map[String,String] => Unit, maxtime: Double=0.0) = {
    errprint("Reading article data from %s...", filename)
    val status = new StatusMessage("article")

    val fi = openr(filename)

    val fields = splittext(fi.next(), '\t')
    breakable {
      for (line <- fi) {
        // println("[%s]" format line)
        val fieldvals = splittext(line, '\t')
        if (fieldvals.length != fields.length)
          warning(
          """Strange record at line #%s, expected %s fields, saw %s fields;
      skipping line=%s""", status.num_processed(), fields.length,
                           fieldvals.length, line)
        else {
          var good = true
          for ((field, value) <- fields zip fieldvals) {
            if (!Article.validate_field(field, value)) {
              good = false
              warning(
          """Bad field value at line #%s, field=%s, value=%s,
      skipping line=%s""", status.num_processed(), field, value, line)
            }
          }
          if (good)
            process((fields zip fieldvals).toMap)
        }
        if (status.item_processed(maxtime=maxtime))
          break
      }
    }
    errprint("Finished reading %s articles.", status.num_processed())
    output_resource_usage()
    fields
  }

  def write_article_data_file(outfile: PrintStream, outfields: Seq[String],
      articles: Iterable[Article]) {
    val writer = new ArticleWriter(outfile, outfields)
    writer.output_header()
    for (art <- articles)
      writer.output_row(art)
    outfile.close()
  }
}

// A Wikipedia article.  Defined fields:
//
//   title: Title of article.
//   id: ID of article, as an int.
//   coord: Coordinates of article.
//   incoming_links: Number of incoming links, or None if unknown.
//   split: Split of article ("training", "dev", "test")
//   redir: If this is a redirect, article title that it redirects to; else
//          an empty string.
//   namespace: Namespace of article (e.g. "Main", "Wikipedia", "File")
//   is_list_of: Whether article title is "List of *"
//   is_disambig: Whether article is a disambiguation page.
//   is_list: Whether article is a list of any type ("List of *", disambig,
//            or in Category or Book namespaces)
class Article(params: Map[String,String]) {
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
  import Article._, ArticleConverters._

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

object Article {
  // Compute the short form of an article name.  If short form includes a
  // division (e.g. "Tucson, Arizona"), return a tuple (SHORTFORM, DIVISION);
  // else return a tuple (SHORTFORM, None).
  
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
    import ArticleConverters._
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
object ArticleConverters {
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
