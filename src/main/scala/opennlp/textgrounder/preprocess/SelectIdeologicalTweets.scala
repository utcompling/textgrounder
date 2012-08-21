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
    help="""Minimum number of political accounts linked to by Twitter users
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
  var include_text = ap.flag("include-text",
    help="""Include text of users sending tweets linking to a potential
    politico.""")
  var ideological_link_type = ap.option[String]("ideological-link-type", "ilt",
    default = "retweets", choices = Seq("retweets", "mentions", "followers"),
    help="""Type of links to other accounts to use when determining the ideology
    of a user.  Possibilities are 'retweets' (accounts that tweets are retweeted
    from); 'mentions' (any @-mention of an account, including retweets);
    'following' (accounts that a user is following).  Default %default.""")
  var politico_link_type = ap.option[String]("politico-link-type", "plt",
    default = "mentions", choices = Seq("retweets", "mentions", "followers"),
    help="""Type of links to other accounts to use when searching for "potential
    politicos" that may be associated with particular ideologies.
    Possibilities are 'retweets' (accounts that tweets are retweeted
    from); 'mentions' (any @-mention of an account, including retweets);
    'following' (accounts that a user is following).  Default %default.""")
}

object SelectIdeologicalTweets extends
    ScoobiProcessFilesApp[SelectIdeologicalTweetsParams] {
  abstract class SelectIdeologicalTweetsShared(opts: SelectIdeologicalTweetsParams)
    extends ScoobiProcessFilesShared {
    val progname = "SelectIdeologicalTweets"
  }

  /**
   * Count of total number of links given a sequence of (account, times) pairs.
   */
  def count_links[T](seq: Seq[(T, Int)]) = seq.map(_._2).sum
  /**
   * Count of total number of accounts given a sequence of (account, times) pairs.
   */
  def count_accounts[T](seq: Seq[(T, Int)]) = seq.length


  /**
   * Description of a "politico" -- a politician along their party and
   * known twitter accounts.
   */
  case class Politico(last: String, first: String, title: String,
      party: String, where: String, accounts: Seq[String]) {
    def full_name = first + " " + last
  }
  implicit val politico_wire =
    mkCaseWireFormat(Politico.apply _, Politico.unapply _)

  /**
   * Description of a user and the accounts linked to, both political and
   * nonpolitical, along with ideology.
   *
   * @param user Twitter account of the user
   * @param ideology Computed ideology of the user (higher values indicate
   *   more conservative)
   * @param ideo_links Set of links to other accounts used in computing
   *   the ideology (either mentions, retweets or following, based on
   *   --ideological-link-type); this is a sequence of tuples of
   *   (account, times), i.e. an account and the number of times it was seen
   * @param lib_ideo_links Subset of `ideo_links` that refer to known
   *   liberal politicos
   * @param cons_ideo_links Subset of `ideo_links` that refer to known
   *   conservative politicos
   * @param politico_links Set of links to other accounts used to generate
   *   potential politicos (either mentions, retweets or following, based
   *   on --politico-link-type)
   * @param text Text of user's tweets (concatenated)
   */
  case class IdeologicalUser(user: String, ideology: Double,
    ideo_links: Seq[(String, Int)],
    lib_ideo_links: Seq[(Politico, Int)],
    cons_ideo_links: Seq[(Politico, Int)],
    politico_links: Seq[(String, Int)],
    text: String) {
    def encode_politico_count_map(seq: Seq[(Politico, Int)]) =
      encode_word_count_map(
        seq.map { case (politico, count) =>
          (politico.full_name.replace(" ", "."), count) })
    def to_row(opts: SelectIdeologicalTweetsParams) =
      Seq(user, "%.3f" format ideology,
        count_accounts(ideo_links), count_links(ideo_links),
          encode_word_count_map(ideo_links),
        count_accounts(politico_links), count_links(politico_links),
          encode_word_count_map(politico_links),
        count_accounts(lib_ideo_links), count_links(lib_ideo_links),
          encode_politico_count_map(lib_ideo_links),
        count_accounts(cons_ideo_links), count_links(cons_ideo_links),
          encode_politico_count_map(cons_ideo_links),
        text
      ) mkString "\t"
  }
  implicit val ideological_user_wire =
    mkCaseWireFormat(IdeologicalUser.apply _, IdeologicalUser.unapply _)

  object IdeologicalUser {

    def row_fields =
      Seq("user", "ideology",
        "num-ideo-accounts", "num-ideo-links", "ideo-links",
        "num-politico-accounts", "num-politico-links", "politico-links",
        "num-lib-ideo-accounts", "num-lib-ideo-links", "lib-ideo-links",
        "num-cons-ideo-accounts", "num-cons-ideo-links", "cons-ideo-links",
        "text")

    /**
     * For a given user, determine if the user is an "ideological user"
     * and if so, return an object describing the user.
     */
    def get_ideological_user(line: String, schema: Schema,
        accounts: Map[String, Politico],
        opts: SelectIdeologicalTweetsParams) = {
      error_wrap(line, None: Option[IdeologicalUser]) { line => {
        val fields = line.split("\t", -1)
        // get list of (links, times) pairs
        val ideo_link_field =
          if (opts.ideological_link_type == "mentions") "user-mentions"
          else opts.ideological_link_type
        val politico_link_field =
          if (opts.politico_link_type == "mentions") "user-mentions"
          else opts.politico_link_type
        val ideo_links =
          decode_word_count_map(schema.get_field(fields, ideo_link_field))
        val politico_links =
          decode_word_count_map(schema.get_field(fields, politico_link_field))
        val text = schema.get_field(fields, "text")
        val user = schema.get_field(fields, "user")
        // errprint("For user %s, ideo_links: %s", user, ideo_links.toList)
        // find links to a politician
        val libcons_ideo_links =
          for {(ideo_link, times) <- ideo_links
               account = accounts.getOrElse(ideo_link.toLowerCase, null)
               if account != null && {
                 //errprint("saw account %s, party %s", account, account.party);
                 "DR".contains(account.party)}}
            yield (account, times)
        //errprint("libcons_ideo_links: %s", libcons_ideo_links.toList)
        val (lib_ideo_links, cons_ideo_links) = libcons_ideo_links.partition {
            case (account, times) => account.party == "D" }
        val num_lib_ideo_links = count_links(lib_ideo_links)
        val num_cons_ideo_links = count_links(cons_ideo_links)
        val num_libcons_ideo_links = num_lib_ideo_links + num_cons_ideo_links
        val ideology = num_cons_ideo_links.toFloat/num_libcons_ideo_links
        if (num_libcons_ideo_links > 0) {
          val ideo_user =
            IdeologicalUser(user, ideology,
              ideo_links, lib_ideo_links, cons_ideo_links, politico_links,
              text)
          Some(ideo_user)
        } else None
      }}
    }
  }

  /**
   * Description of a potential politico -- someone linked to by an
   * ideological user.
   *
   * @param lcuser Twitter account of user, lowercased
   * @param spellings Map of actual (non-lowercased) spellings of account
   *   by usage
   * @param num_accounts Total number of accounts linking to user
   * @param num_links Total number of links to user
   * @param num_lib_accounts Number of accounts with a noticeably
   *   "liberal" ideology linking to user
   * @param num_lib_links Number of links to user from accounts with a
   *   noticeably "liberal" ideology
   * @param num_cons_accounts Number of accounts with a noticeably
   *   "conservative" ideology linking to user
   * @param num_cons_links Number of links to user from accounts with a
   *   noticeably "conservative" ideology
   * @param num_links_ideo_weighted Sum of links weighted by ideology of
   *   person doing the linking, so that we can compute a weighted
   *   average to determine their ideology.
   * @param all_text Text of all users linking to the politico.
   */
  case class PotentialPolitico(lcuser: String, spellings: Map[String, Int],
    num_accounts: Int, num_links: Int,
    num_lib_accounts: Int, num_lib_links: Int,
    num_cons_accounts: Int, num_cons_links: Int,
    num_links_ideo_weighted: Double, all_text: Seq[String]) {
    def to_row(opts: SelectIdeologicalTweetsParams) =
      Seq(lcuser, encode_word_count_map(spellings.toSeq),
        num_accounts, num_links,
        num_lib_accounts, num_lib_links,
        num_cons_accounts, num_cons_links,
        num_links_ideo_weighted/num_links,
        if (opts.include_text) all_text mkString " !! " else "(omitted)"
      ) mkString "\t"
  }

  object PotentialPolitico {

    def row_fields =
      Seq("lcuser", "spellings", "num-accounts", "num-links",
        "num-lib-accounts", "num-lib-links",
        "num-cons-accounts", "num-cons-links",
        "ideology", "all-text")
    /**
     * For a given ideological user, generate the "potential politicos": other
     * people linked to, along with their ideological scores.
     */
    def get_potential_politicos(user: IdeologicalUser,
        opts: SelectIdeologicalTweetsParams) = {
      for {(link, times) <- user.politico_links
           lclink = link.toLowerCase } yield {
        val is_lib = user.ideology <= opts.max_liberal
        val is_conserv = user.ideology >= opts.min_conservative
        PotentialPolitico(
          lclink, Map(link->times), 1, times,
          if (is_lib) 1 else 0,
          if (is_lib) times else 0,
          if (is_conserv) 1 else 0,
          if (is_conserv) times else 0,
          times * user.ideology,
          Seq(user.text)
        )
      }
    }

    /**
     * Merge two PotentialPolitico objects, which must refer to the same user.
     * Add up the links and combine the set of spellings.
     */
    def merge_potential_politicos(u1: PotentialPolitico, u2: PotentialPolitico) = {
      assert(u1.lcuser == u2.lcuser)
      PotentialPolitico(u1.lcuser, combine_maps(u1.spellings, u2.spellings),
        u1.num_accounts + u2.num_accounts,
        u1.num_links + u2.num_links,
        u1.num_lib_accounts + u2.num_lib_accounts,
        u1.num_lib_links + u2.num_lib_links,
        u1.num_cons_accounts + u2.num_cons_accounts,
        u1.num_cons_links + u2.num_cons_links,
        u1.num_links_ideo_weighted + u2.num_links_ideo_weighted,
        u1.all_text ++ u2.all_text)
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
        """^([^ .]+)\. (.*?), (.*?) (-+ |(?:@[^ ]+ )+)([RDI?]) \((.*)\)$""".r
      val all_accounts =
        // Open the file and read line by line.
        for ((line, lineind) <- (new LocalFileHandler).openr(filename).zipWithIndex
             // Skip comments and blank lines
             if !line.startsWith("#") && !(line.trim.length == 0)) yield {
          lineno = lineind + 1
          line match {
            // Match the line.
            case politico(title, last, first, accountstr, party, where) => {
              // Compute the list of normalized accounts.
              val accounts =
                if (accountstr.startsWith("-")) Seq[String]()
                // `tail` removes the leading @; lowercase to normalize
                else accountstr.split(" ").map(_.tail.toLowerCase).toSeq
              val obj = Politico(last, first, title, party, where, accounts)
              for (account <- accounts) yield (account, obj)
            }
            case _ => {
              warning(line, "Unable to match")
              Seq[(String, Politico)]()
            }
          }
        }
      lineno = 0
      // For each account read in, we generated multiple pairs; flatten and
      // convert to a map.  Reverse because the last of identical keys will end
      // up in the map but we want the first one taken.
      all_accounts.flatten.toSeq.reverse.toMap
    }


    /*
     2. We go through users looking for links to these politicians.  For
        users that link politicians, we can compute an "ideology" score of
        the user by a weighted average of the links by the ideology of
        the politicians.
     3. For each such user, look at all other people linked to -- the idea is
        we want to look for people linked to a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were linked to and an ideology score
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
     2. We go through users looking for links to these politicians.  For
        users that link to politicians, we can compute an "ideology" score of
        the user by a weighted average of the links by the ideology of
        the politicians.
     3. For each such user, look at all other people linked to -- the idea is
        we want to look for people linked to a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were linked to and an ideology score
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

    def output_lines(lines: DList[String], corpus_suffix: String,
        fields: Seq[String]) {
      val outdir = opts.output + "-" + corpus_suffix
      persist(TextOutput.toTextFile(lines, outdir))
      val out_schema = new Schema(fields, Map("corpus" -> opts.corpus_name))
      val out_schema_fn = Schema.construct_schema_file(filehand,
          outdir, opts.corpus_name, corpus_suffix)
      rename_output_files(configuration.fs, outdir, opts.corpus_name,
        corpus_suffix)
      out_schema.output_schema_file(filehand, out_schema_fn)
    }

    errprint("Step 1: Load corpus, filter for conservatives/liberals, output.")
    val matching_patterns = CorpusFileProcessor.
        get_matching_patterns(filehand, opts.input, suffix)
    val lines: DList[String] = TextInput.fromTextFile(matching_patterns: _*)
    val ideo_users =
      lines.flatMap(
        IdeologicalUser.get_ideological_user(_, in_schema, accounts, opts))
    output_lines(ideo_users.map(_.to_row(opts)), "ideo-users",
      IdeologicalUser.row_fields)
    errprint("Step 1: done.")

    errprint("Step 2: Generate potential politicos.")
    val potential_politicos = ideo_users.
      flatMap(PotentialPolitico.get_potential_politicos(_, opts)).
      groupBy(_.lcuser).
      combine(PotentialPolitico.merge_potential_politicos).
      map(_._2)
    output_lines(potential_politicos.map(_.to_row(opts)),
      "potential-politicos", PotentialPolitico.row_fields)
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

