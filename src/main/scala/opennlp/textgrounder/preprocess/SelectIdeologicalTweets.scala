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
import tgutil.collectionutil._
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
  var min_conservative = ap.option[Double]("min-conservative", "mc",
    default = 0.75,
    help="""Minimum ideology score to consider a user as an "ideological
    conservative".  On the ideology scale, greater values indicate more
    conservative.  Currently, the scale runs from 0 to 1; hence, this value
    should be greater than 0.5.  Default %default.""")
  var max_liberal = ap.option[Double]("max-liberal", "ml",
    help="""Maximum ideology score to consider a user as an "ideological
    liberal".  On the ideology scale, greater values indicate more
    conservative.  Currently, the scale runs from 0 to 1; hence, this value
    should be less than 0.5.  If unspecified, computed as the mirror image of
    the value of '--min-conservative' (e.g. 0.25 if
    --min-conservative=0.75).""")
  var corpus_name = ap.option[String]("corpus-name",
    help="""Name of output corpus; for identification purposes.
    Default to name taken from input directory.""")
}

object SelectIdeologicalTweets extends
    ScoobiProcessFilesApp[SelectIdeologicalTweetsParams] {
  abstract class SelectIdeologicalTweetsShared(opts: SelectIdeologicalTweetsParams)
    extends ScoobiProcessFilesShared {
    val progname = "SelectIdeologicalTweets"
  }

  /**
   * Description of a "politico" -- a politician along their party and
   * known twitter accounts.
   */
  case class Politico(last: String, first: String, title: String,
      party: String, where: String, accounts: Set[String]) {
    def full_name = first + " " + last
  }
  implicit val politico_wire =
    mkCaseWireFormat(Politico.apply _, Politico.unapply _)

  /**
   * Description of a user and the accounts mentioned, both political and
   * nonpolitical, along with ideology.
   */
  case class IdeologicalUser(user: String, ideology: Double,
    num_mentions: Int, mentions: Seq[(String, Int)],
    num_dem_mentions: Int, dem_mentions: Seq[(Politico, Int)],
    num_rep_mentions: Int, rep_mentions: Seq[(Politico, Int)]) {
    def encode_politico_count_map(seq: Seq[(Politico, Int)]) =
      encode_word_count_map(
        seq.map { case (politico, count) =>
          (politico.full_name.replace(" ", "."), count) })
    def to_row =
      Seq(user, "%.3f" format ideology,
        num_mentions.toString, encode_word_count_map(mentions),
        num_dem_mentions.toString, encode_politico_count_map(dem_mentions),
        num_rep_mentions.toString, encode_politico_count_map(rep_mentions)
      ) mkString "\t"
  }
  implicit val ideological_user_wire =
    mkCaseWireFormat(IdeologicalUser.apply _, IdeologicalUser.unapply _)

  object IdeologicalUser {

    def row_fields =
      Seq("user", "ideology", "num-mentions", "mentions",
        "num-dem-mentions", "dem-mentions",
        "num-rep-mentions", "rep-mentions")

    /**
     * For a given user, determine if the user is an "ideological user"
     * and if so, return an object describing the user.
     */
    def get_ideological_user(line: String, schema: Schema,
        accounts: Map[String, Politico]) = {
      error_wrap(line, None: Option[IdeologicalUser]) { line => {
        val fields = line.split("\t", -1)
        // get list of (user mentions, times) pairs
        val mentions =
          decode_word_count_map(schema.get_field(fields, "user-mentions"))
        val num_mentions = mentions.map(_._2).sum
        val user = schema.get_field(fields, "user")
        // errprint("For user %s, mentions: %s", user, mentions.toList)
        // find mentions of a politician
        val demrep_mentions =
          for {(mention, times) <- mentions
               account = accounts.getOrElse(mention.toLowerCase, null)
               if account != null && {
                 //errprint("saw account %s, party %s", account, account.party);
                 "DR".contains(account.party)}}
            yield (account, times)
        //errprint("demrep_mentions: %s", demrep_mentions.toList)
        val (dem_mentions, rep_mentions) = demrep_mentions.partition {
            case (account, times) => account.party == "D" }
        val num_dem_mentions = dem_mentions.map(_._2).sum
        val num_rep_mentions = rep_mentions.map(_._2).sum
        val num_demrep_mentions = num_dem_mentions + num_rep_mentions
        val ideology = num_rep_mentions.toFloat/num_demrep_mentions
        if (num_demrep_mentions > 0) {
          val ideo_user =
            IdeologicalUser(user, ideology,
              num_mentions, mentions,
              num_dem_mentions, dem_mentions,
              num_rep_mentions, rep_mentions)
          Some(ideo_user)
        } else None
      }}
    }
  }

  /**
   * Description of a potential politico -- someone mentioned by an
   * ideological user.
   *
   * @param lcuser Twitter account of user, lowercased
   * @param spellings Map of actual (non-lowercased) spellings of account
   *   by usage
   * @param num_mentions Total number of mentions
   *   a noticeably "liberal" ideology
   * @param num_lib_mentions Number of times mentioned by people with
   *   a noticeably "liberal" ideology
   * @param num_conserv_mentions Number of times mentioned by people with
   *   a noticeably "conservative" ideology
   * @param num_ideo_mentions Sum of mentions weighted by ideology of
   *   person doing the mentioning, so that we can compute a weighted
   *   average to determine their ideology.
   */
  case class PotentialPolitico(lcuser: String, spellings: Map[String, Int],
    num_mentions: Int, num_lib_mentions: Int, num_conserv_mentions: Int,
    num_ideo_mentions: Double) {
    def to_row =
      Seq(lcuser, encode_word_count_map(spellings.toSeq), num_mentions,
        num_lib_mentions, num_conserv_mentions,
        num_ideo_mentions/num_mentions) mkString "\t"
  }

  object PotentialPolitico {

    def row_fields =
      Seq("lcuser", "spellings", "num-mentions", "num-lib-mentions",
        "num-conserv-mentions", "ideology")
    /**
     * For a given ideological user, generate the "potential politicos": other
     * people mentioned, along with their ideological scores.
     */
    def get_potential_politicos(user: IdeologicalUser,
        opts: SelectIdeologicalTweetsParams) = {
      for {(mention, times) <- user.mentions
           lcmention = mention.toLowerCase } yield {
        val is_lib = user.ideology <= opts.max_liberal
        val is_conserv = user.ideology >= opts.min_conservative
        PotentialPolitico(
          lcmention, Map(mention->times), times,
          if (is_lib) times else 0,
          if (is_conserv) times else 0,
          times * user.ideology
        )
      }
    }

    /**
     * Merge two PotentialPolitico objects, which must refer to the same user.
     * Add up the mentions and combine the set of spellings.
     */
    def merge_potential_politicos(u1: PotentialPolitico, u2: PotentialPolitico) = {
      assert(u1.lcuser == u2.lcuser)
      PotentialPolitico(u1.lcuser, combine_maps(u1.spellings, u2.spellings),
        u1.num_mentions + u2.num_mentions,
        u1.num_lib_mentions + u2.num_lib_mentions,
        u1.num_conserv_mentions + u2.num_conserv_mentions,
        u1.num_ideo_mentions + u2.num_ideo_mentions)
    }
  }

  implicit val potential_politico =
    mkCaseWireFormat(PotentialPolitico.apply _, PotentialPolitico.unapply _)

  class SelectIdeologicalTweets(opts: SelectIdeologicalTweetsParams)
      extends SelectIdeologicalTweetsShared(opts) {
    val operation_category = "Driver"

    /**
     * Read the set of ideological accounts.  Create a "Politico" object for
     * each such account, and return a map from a normalized (lowercased)
     * version of each account to the corresponding Politico object (which
     * may refer to multiple accounts).
     */
    def read_ideological_accounts(filename: String) = {
      val politico =
        """^(Rep|Sen|Gov)\. (.*?), (.*?) (-+ |(?:@[^ ]+ )+)([RDI?]) \((.*)\)$""".r
      val all_accounts =
        // Open the file and read line by line.
        for (line <- (new LocalFileHandler).openr(filename)) yield {
          lineno += 1
          line match {
            // Match the line.
            case politico(title, last, first, accountstr, party, where) => {
              // Compute the set of normalized accounts.
              val accounts =
                if (accountstr.startsWith("-")) Set[String]()
                // `tail` removes the leading @; lowercase to normalize
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
      lineno = 0
      // For each account read in, we generated multiple pairs; flatten and
      // convert to a map.
      all_accounts.flatten.toMap
    }


    /*
     2. We go through users looking for mentions of these politicians.  For
        users that mention politicians, we can compute an "ideology" score of
        the user by a weighted average of the mentions by the ideology of
        the politicians.
     3. For each such user, look at all other people mentioned -- the idea is
        we want to look for people mentioned a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were mentioned and an ideology score
        and merge these all together.
     */
  }

  def create_params(ap: ArgParser) = new SelectIdeologicalTweetsParams(ap)
  val progname = "SelectIdeologicalTweets"

  def run() {
    // For testing
    // errprint("Calling error_wrap ...")
    // error_wrap(1,0) { _ / 0 }
    val opts = init_scoobi_app()
    /*
     We are doing the following:

     1. We are given a list of known politicians, their twitter accounts, and
        their ideology -- either determined simply by their party, or using
        the DW-NOMINATE score or similar.
     2. We go through users looking for mentions of these politicians.  For
        users that mention politicians, we can compute an "ideology" score of
        the user by a weighted average of the mentions by the ideology of
        the politicians.
     3. For each such user, look at all other people mentioned -- the idea is
        we want to look for people mentioned a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were mentioned and an ideology score
        and merge these all together.
     */
    val ptp = new SelectIdeologicalTweets(opts)
    val filehand = new HadoopFileHandler(configuration)
    if (opts.political_twitter_accounts == null) {
      opts.ap.error("--political-twitter-accounts must be specified")
    }
    if (!opts.ap.specified("max-liberal"))
      opts.max_liberal = 1 - opts.min_conservative
    if (opts.corpus_name == null) {
      val (_, last_component) = filehand.split_filename(opts.input)
      opts.corpus_name = last_component.replace("*", "_")
    }
    val accounts = ptp.read_ideological_accounts(opts.political_twitter_accounts)
    // errprint("Accounts: %s", accounts)
    val suffix = "counts"
    val in_schema_file =
      CorpusFileProcessor.find_schema_file(filehand, opts.input, suffix)
    val in_schema = Schema.read_schema_file(filehand, in_schema_file)

    errprint("Step 1: Load corpus, filter for conservatives/liberals, output.")
    val matching_patterns = CorpusFileProcessor.
        get_matching_patterns(filehand, opts.input, suffix)
    val lines: DList[String] = TextInput.fromTextFile(matching_patterns: _*)
    val ideo_users =
      lines.flatMap(
        IdeologicalUser.get_ideological_user(_, in_schema, accounts))
    val outlines1 = ideo_users.map(_.to_row)
    val corp_suffix1 = "ideo-users"
    val outdir1 = opts.output + "-" + corp_suffix1
    persist(TextOutput.toTextFile(outlines1, outdir1))
    val out_schema1 = new Schema(IdeologicalUser.row_fields,
      Map("corpus" -> opts.corpus_name))
    val out_schema1_fn = Schema.construct_schema_file(filehand,
        outdir1, opts.corpus_name, corp_suffix1)
    out_schema1.output_schema_file(filehand, out_schema1_fn)
    errprint("Step 1: done.")

    errprint("Step 2: Generate potential politicos.")
    val potential_politicos = ideo_users.
      flatMap(PotentialPolitico.get_potential_politicos(_, opts)).
      groupBy(_.lcuser).
      combine(PotentialPolitico.merge_potential_politicos).
      map(_._2)
    val outlines2 = potential_politicos.map(_.to_row)
    val corp_suffix2 = "potential-politicos"
    val outdir2 = opts.output + "-" + corp_suffix2
    persist(TextOutput.toTextFile(outlines2, outdir2))
    val out_schema2 = new Schema(PotentialPolitico.row_fields,
      Map("corpus" -> opts.corpus_name))
    val out_schema2_fn = Schema.construct_schema_file(filehand,
        outdir2, opts.corpus_name, corp_suffix2)
    out_schema2.output_schema_file(filehand, out_schema2_fn)
    errprint("Step 2: done.")

    finish_scoobi_app(opts)
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

