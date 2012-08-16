//  SelectIdeologicalTweets.scala
//
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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

import org.apache.commons.logging

import com.nicta.scoobi.Scoobi._

import opennlp.textgrounder.{util => tgutil}
import tgutil.argparser._
import tgutil.corpusutil._
import tgutil.hadoop._
import tgutil.ioutil._
import tgutil.osutil._
import tgutil.printutil._

class SelectIdeologicalTweetsParams(val ap: ArgParser) extends
    ScoobiProcessFilesParams(ap) {
  var political_twitter_accounts = ap.option[String](
    "political-twitter-accounts", "pta",
    help="""File containing list of politicians and associated twitter
    accounts, for identifying liberal and conservative tweeters.""")
  var min_accounts = ap.option[Int]("min-accounts", default = 2,
    help="""Minimum number of political accounts mentioned by Twitter users
in order for users to be considered.  Default %default.""")
}

object SelectIdeologicalTweets extends
    ScoobiProcessFilesApp[SelectIdeologicalTweetsParams] {
  abstract class SelectIdeologicalTweetsShared(Opts: SelectIdeologicalTweetsParams)
    extends ScoobiProcessFilesShared {
    val progname = "SelectIdeologicalTweets"
  }

  case class Politico(last: String, first: String, title: String,
    party: String, where: String, accounts: Set[String])
  implicit val politico_wire =
    mkCaseWireFormat(Politico.apply _, Politico.unapply _)

  class SelectIdeologicalTweets(Opts: SelectIdeologicalTweetsParams)
      extends SelectIdeologicalTweetsShared(Opts) {
    val operation_category = "Driver"

    /**
     * Output a schema file of the appropriate name.
     */
    def read_ideological_accounts(filename: String) = {
      val politico =
        """^(Rep|Sen|Gov)\. (.*?), (.*?) (-+ |(?:@[^ ]+ )+)([RDI?]) \((.*)\)$""".r
      val all_accounts =
        for (line <- (new LocalFileHandler).openr(filename)) yield {
          lineno += 1
          line match {
            case politico(title, last, first, accountstr, party, where) => {
              val accounts =
                if (accountstr.startsWith("-")) Set[String]()
                else accountstr.split(" ").map(_.tail.toLowerCase).toSet
              val obj = Politico(last, first, title, party, where, accounts)
              for (account <- accounts) yield (account, obj)
            }
            case _ => {
              warning(line, "Unable to match")
              Set[(String, Politico)]()
            }
          }
        }
      all_accounts.flatMap(identity).toMap
    }

    def tweet_by_ideology(schema: Schema, line: String,
        accounts: Map[String, Politico]) = {
      error_wrap(line, "")(line =>
      {
      val fields = line.split("\t", -1)
      val mentions =
        decode_word_count_map(schema.get_field(fields, "user-mentions")).map {
          case (account, times) => (account.toLowerCase, times) }
      val user = schema.get_field(fields, "user")
      // errprint("For user %s, mentions: %s", user, mentions.toList)
      val demrep_mentions =
        for {(mention, times) <- mentions
             account = accounts.getOrElse(mention, null)
             if account != null && {
               //errprint("saw account %s, party %s", account, account.party);
               "DR".contains(account.party)}}
          yield (account, times)
      //errprint("demrep_mentions: %s", demrep_mentions.toList)
      val (dem_mentions, rep_mentions) = demrep_mentions.partition {
          case (account, times) => account.party == "D" }
      def sum_values[T](seq: Seq[(T, Int)]) = seq.map(_._2).sum
      val num_demrep_mentions = sum_values(demrep_mentions)
      val num_dem_mentions = sum_values(dem_mentions)
      if (num_demrep_mentions > 0)
        "user %s, %d/%d = %.1f%% Democrat, dems: %s, reps: %s, line: %s" format (
          user, num_dem_mentions, num_demrep_mentions,
          100*num_dem_mentions.toFloat/num_demrep_mentions,
          dem_mentions.toList, rep_mentions.toList, line)
      else
        ""
    })
    }
  }

  def create_params(ap: ArgParser) = new SelectIdeologicalTweetsParams(ap)
  val progname = "SelectIdeologicalTweets"

  def run() {
    val Opts = init_scoobi_app()
    val ptp = new SelectIdeologicalTweets(Opts)
    if (Opts.political_twitter_accounts == null) {
      Opts.ap.error("--political-twitter-accounts must be specified")
    }
    val accounts = ptp.read_ideological_accounts(Opts.political_twitter_accounts)
    // errprint("Accounts: %s", accounts)
    val filehand = new HadoopFileHandler(configuration)
    val suffix = "counts"
    val schema_file =
      CorpusFileProcessor.find_schema_file(filehand, Opts.input, suffix)
    val schema = Schema.read_schema_file(filehand, schema_file)

    errprint("Step 1: Load corpus, filter for conservatives/liberals, output.")

    // Firstly we load up all the (new-line-separated) JSON lines.
    val matching_patterns = CorpusFileProcessor.
        get_matching_patterns(filehand, Opts.input, suffix)
    errprint("matching patterns: %s", matching_patterns)
    val lines: DList[String] = TextInput.fromTextFile(matching_patterns: _*)
    val outlines = lines.map(ptp.tweet_by_ideology(schema, _, accounts)).
        filter(_.length > 0)
    persist(TextOutput.toTextFile(outlines, Opts.output))
    errprint("Step 1: done.")

    finish_scoobi_app(Opts)
  }
  /*

  To build a classifier for conserv vs liberal:

  1. Look for people retweeting congressmen or governor tweets, possibly at
     some minimum level of retweeting (or rely on followers, for some
     minimum number of people following)
  2. Make sure either they predominate having retweets from one party,
     and/or use the DW-NOMINATE scores to pick out people whose average
     ideology score of their retweets is near the extremes.
  */
}

