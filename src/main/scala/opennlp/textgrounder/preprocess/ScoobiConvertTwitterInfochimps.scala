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

import java.io._
import java.io.{FileSystem=>_,_}
import util.control.Breaks._

import org.apache.hadoop.io._
import org.apache.hadoop.util._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs._

import com.nicta.scoobi.Scoobi._

/*

Steps for converting Infochimps to our format:

1) Input is a series of files, e.g. part-00000.gz, each about 180 MB.
2) Each line looks like this:


100000018081132545      20110807002716  25430513        GTheHardWay                                     Niggas Lost in the Sauce ..smh better slow yo roll and tell them hoes to get a job nigga #MRIloveRatsIcanchange&amp;amp;saveherassNIGGA &lt;a href=&quot;http://twitter.com/download/android&quot; rel=&quot;nofollow&quot;&gt;Twitter for Android&lt;/a&gt;    en      42.330165       -83.045913                                      
The fields are:

1) Tweet ID
2) Time
3) User ID
4) User name
5) Empty?
6) User name being replied to (FIXME: which JSON field is this?)
7) User ID for replied-to user name (but sometimes different ID's for same user name)
8) Empty?
9) Tweet text -- double HTML-encoded (e.g. & becomes &amp;amp;)
10) HTML anchor text indicating a link of some sort, HTML-encoded (FIXME: which JSON field is this?)
11) Language, as a two-letter code
12) Latitude
13) Longitude
14) Empty?
15) Empty?
16) Empty?
17) Empty?


3) We want to convert each to two files: (1) containing the article-data 

*/

/**
 * Convert files in the Infochimps Twitter corpus into files in our format.
 */

object ScoobiConvertTwitterInfochimps extends ScoobiApp {

  def usage() {
    sys.error("""Usage: ConvertTwitterInfochimps INFILE OUTDIR

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  INFILE is a single file or a glob.
OUTDIR is the directory to store the results in.
""")
  }

  def run() {
    if (args.length != 2)
      usage()
    val infile = args(0)
    val outdir = args(1)

    val fields = List("id", "title", "split", "coord", "time",
      "username", "userid", "reply_username", "reply_userid", "anchor", "lang")

    val tweets = fromDelimitedTextFile("\t", infile) {
      case id :: time :: userid :: username :: _ ::
          reply_username :: reply_userid :: _ :: text :: anchor :: lang ::
          lat :: long :: _ :: _ :: _ :: _ => {
        val metadata = List(id, id, "training", "%s,%s" format (lat, long),
          time, username, userid, reply_username, reply_userid, anchor, lang)
        val splittext = text.split(" ")
        val textdata = List(id, id, splittext)
        (metadata mkString "\t", textdata mkString "\t")
      }
    }
    persist (
      TextOutput.toTextFile(tweets.map(_._1),
        "%s-twitter-infochimps-combined-document-data.txt" format outdir),
      TextOutput.toTextFile(tweets.map(_._2),
        "%s-twitter-infochimps-text-data.txt" format outdir)
    )
  }
}
