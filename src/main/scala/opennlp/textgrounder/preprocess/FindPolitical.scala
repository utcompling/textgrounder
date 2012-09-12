//  FindPolitical.scala
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

class FindPoliticalParams(ap: ArgParser) extends
    ScoobiProcessFilesParams(ap) {
  var political_twitter_accounts = ap.option[String](
    "political-twitter-accounts", "pta",
    help="""File containing list of politicians and associated twitter
    accounts, for identifying liberal and conservative tweeters.""")
  var min_accounts = ap.option[Int]("min-accounts", default = 2,
    help="""Minimum number of political accounts referenced by Twitter users
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
    help="""Include text of users sending tweets referencing a feature.""")
  var ideological_ref_type = ap.option[String]("ideological-ref-type", "ilt",
    default = "retweets", choices = Seq("retweets", "mentions", "followers"),
    help="""Type of references to other accounts to use when determining the
    ideology of a user.  Possibilities are 'retweets' (accounts that tweets
    are retweeted from); 'mentions' (any @-mention of an account, including
    retweets); 'following' (accounts that a user is following).  Default
    %default.""")
  var political_feature_type = ap.multiOption[String]("political-feature-type",
    "pft",
    choices = Seq("retweets", "followers", "hashtags", "urls", "images",
      "unigrams", "bigrams", "trigrams", "ngrams"),
    aliasedChoices = Seq(Seq("user-mentions", "mentions")),
    help="""Type of political features to track when searching for data that may
    be associated with particular ideologies.  Possibilities are 'retweets'
    (accounts that tweets are retweeted from); 'mentions' (any @-mention of an
    account, including retweets); 'following' (accounts that a user is
    following); 'hashtags'; 'unigrams'; 'bigrams'; 'trigrams'; 'ngrams'.
    DOCUMENT THESE; NOT YET IMPLEMENTED. Multiple features can be tracked
    simultaneously by specifying this option multiple times.""")
    // FIXME: Should be able to specify multiple features separated by commas.
    // This requires that we fix argparser.scala to allow this.  Probably
    // should add an extra field to provide a way of splitting -- maybe a regexp,
    // maybe a function.
  // Schema for the input file, after file read
  var schema: Schema = _
}

object FindPolitical extends
    ScoobiProcessFilesApp[FindPoliticalParams] {
  abstract class FindPoliticalAction(opts: FindPoliticalParams)
    extends ScoobiProcessFilesAction {
    val progname = "FindPolitical"
  }

  /**
   * Count of total number of references given a sequence of (data, times) pairs
   * of references to a particular data point.
   */
  def count_refs[T](seq: Seq[(T, Int)]) = seq.map(_._2).sum
  /**
   * Count of total number of accounts given a sequence of (data, times) pairs
   * of references to a particular data point.
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
   * Description of a user and the accounts referenced, both political and
   * nonpolitical, along with ideology.
   *
   * @param user Twitter account of the user
   * @param ideology Computed ideology of the user (higher values indicate
   *   more conservative)
   * @param ideo_refs Set of references to other accounts used in computing
   *   the ideology (either mentions, retweets or following, based on
   *   --ideological-ref-type); this is a sequence of tuples of
   *   (account, times), i.e. an account and the number of times it was seen
   * @param lib_ideo_refs Subset of `ideo_refs` that refer to known
   *   liberal politicos
   * @param cons_ideo_refs Subset of `ideo_refs` that refer to known
   *   conservative politicos
   * @param text Text of user's tweets (concatenated)
   */
  case class IdeologicalUser(user: String, ideology: Double,
      ideo_refs: Seq[(String, Int)],
      lib_ideo_refs: Seq[(Politico, Int)],
      cons_ideo_refs: Seq[(Politico, Int)],
      fields: Seq[String]) {
    def encode_politico_count_map(seq: Seq[(Politico, Int)]) =
      encode_word_count_map(
        seq.map { case (politico, count) =>
          (politico.full_name.replace(" ", "."), count) })

    def get_feature_values(factory: IdeologicalUserAction, ty: String) = {
      ty match {
        case field@("retweets" | "user-mentions" | "hashtags") =>
          decode_word_count_map(
            factory.user_subschema.get_field(fields, field))
        // case "followers" => FIXME
        // case "unigrams" => FIXME
        // case "bigrams" => FIXME
        // case "trigrams" => FIXME
        // case "ngrams" => FIXME
      }
    }

    def to_row(opts: FindPoliticalParams) =
      Seq(user, "%.3f" format ideology,
        count_accounts(ideo_refs), count_refs(ideo_refs),
          encode_word_count_map(ideo_refs),
        count_accounts(lib_ideo_refs), count_refs(lib_ideo_refs),
          encode_politico_count_map(lib_ideo_refs),
        count_accounts(cons_ideo_refs), count_refs(cons_ideo_refs),
          encode_politico_count_map(cons_ideo_refs),
        fields mkString "\t"
      ) mkString "\t"
  }
  implicit val ideological_user_wire =
    mkCaseWireFormat(IdeologicalUser.apply _, IdeologicalUser.unapply _)

  class IdeologicalUserAction(opts: FindPoliticalParams) extends
      FindPoliticalAction(opts) {
    val operation_category = "IdeologicalUser"

    val user_subschema_fieldnames =
      opts.schema.fieldnames filterNot (_ == "user")
    val user_subschema = new SubSchema(user_subschema_fieldnames,
      opts.schema.fixed_values, opts.schema)
 
    def row_fields =
      Seq("user", "ideology",
        "num-ideo-accounts", "num-ideo-refs", "ideo-refs",
        "num-lib-ideo-accounts", "num-lib-ideo-refs", "lib-ideo-refs",
        "num-cons-ideo-accounts", "num-cons-ideo-refs", "cons-ideo-refs",
        "fields")

    /**
     * For a given user, determine if the user is an "ideological user"
     * and if so, return an object describing the user.
     */
    def get_ideological_user(line: String, accounts: Map[String, Politico]) = {
      error_wrap(line, None: Option[IdeologicalUser]) { line => {
        val fields = line.split("\t", -1)
        // get list of (refs, times) pairs
        val ideo_ref_field =
          if (opts.ideological_ref_type == "mentions") "user-mentions"
          else opts.ideological_ref_type
        val ideo_refs =
          decode_word_count_map(opts.schema.get_field(fields, ideo_ref_field))
        val text = opts.schema.get_field(fields, "text")
        val user = opts.schema.get_field(fields, "user")
        // errprint("For user %s, ideo_refs: %s", user, ideo_refs.toList)
        // find references to a politician
        val libcons_ideo_refs =
          for {(ideo_ref, times) <- ideo_refs
               account = accounts.getOrElse(ideo_ref.toLowerCase, null)
               if account != null && {
                 //errprint("saw account %s, party %s", account, account.party);
                 "DR".contains(account.party)}}
            yield (account, times)
        //errprint("libcons_ideo_refs: %s", libcons_ideo_refs.toList)
        val (lib_ideo_refs, cons_ideo_refs) = libcons_ideo_refs.partition {
            case (account, times) => account.party == "D" }
        val num_lib_ideo_refs = count_refs(lib_ideo_refs)
        val num_cons_ideo_refs = count_refs(cons_ideo_refs)
        val num_libcons_ideo_refs = num_lib_ideo_refs + num_cons_ideo_refs
        val ideology = num_cons_ideo_refs.toFloat/num_libcons_ideo_refs
        if (num_libcons_ideo_refs > 0) {
          val ideo_user =
            IdeologicalUser(user, ideology, ideo_refs, lib_ideo_refs,
            cons_ideo_refs, user_subschema.map_original_fieldvals(fields))
          Some(ideo_user)
        } else None
      }}
    }
  }

  /**
   * A political data point -- a piece of data (e.g. user mention, retweet,
   * hash tag, URL, n-gram, etc.) in a tweet by an ideological user.
   *
   * @param data Data of the data point
   * @param ty Type of data point
   * @param spellings Map of actual (non-lowercased) spellings of data point
   *   by usage
   * @param num_accounts Total number of accounts referencing data point
   * @param num_refs Total number of references to data point
   * @param num_lib_accounts Number of accounts with a noticeably
   *   "liberal" ideology referencing data point
   * @param num_lib_refs Number of references to data point from accounts
   *   with a noticeably "liberal" ideology
   * @param num_cons_accounts Number of accounts with a noticeably
   *   "conservative" ideology referencing data point
   * @param num_cons_refs Number of references to data point from accounts
   *   with a noticeably "conservative" ideology
   * @param num_refs_ideo_weighted Sum of references weighted by ideology of
   *   person doing the referenceing, so that we can compute a weighted
   *   average to determine their ideology.
   * @param num_mentions Total number of mentions
   * @param num_lib_mentions Number of times mentioned by people with
   *   a noticeably "liberal" ideology
   * @param num_conserv_mentions Number of times mentioned by people with
   *   a noticeably "conservative" ideology
   * @param num_ideo_mentions Sum of mentions weighted by ideology of
   *   person doing the mentioning, so that we can compute a weighted
   *   average to determine their ideology.
   * @param all_text Text of all users referencing the politico.
   */
  case class PoliticalFeature(value: String, spellings: Map[String, Int],
    num_accounts: Int, num_refs: Int,
    num_lib_accounts: Int, num_lib_refs: Int,
    num_cons_accounts: Int, num_cons_refs: Int,
    num_refs_ideo_weighted: Double, all_text: Seq[String]) {
    def to_row(opts: FindPoliticalParams) =
      Seq(value, encode_word_count_map(spellings.toSeq),
        num_accounts, num_refs,
        num_lib_accounts, num_lib_refs,
        num_cons_accounts, num_cons_refs,
        num_refs_ideo_weighted/num_refs,
        if (opts.include_text) all_text mkString " !! " else "(omitted)"
      ) mkString "\t"
  }

  object PoliticalFeature {

    def row_fields =
      Seq("value", "spellings", "num-accounts", "num-refs",
        "num-lib-accounts", "num-lib-refs",
        "num-cons-accounts", "num-cons-refs",
        "ideology", "all-text")
    /**
     * For a given ideological user, generate the "potential politicos": other
     * people referenced, along with their ideological scores.
     */
    def get_political_features(factory: IdeologicalUserAction,
        user: IdeologicalUser, ty: String,
        opts: FindPoliticalParams) = {
      for {(ref, times) <- user.get_feature_values(factory, ty)
           lcref = ref.toLowerCase } yield {
        val is_lib = user.ideology <= opts.max_liberal
        val is_conserv = user.ideology >= opts.min_conservative
        PoliticalFeature(
          lcref, Map(ref->times), 1, times,
          if (is_lib) 1 else 0,
          if (is_lib) times else 0,
          if (is_conserv) 1 else 0,
          if (is_conserv) times else 0,
          times * user.ideology,
          Seq("FIXME fill-in text maybe")
        )
      }
    }

    /**
     * Merge two PoliticalFeature objects, which must refer to the same user.
     * Add up the references and combine the set of spellings.
     */
    def merge_political_features(u1: PoliticalFeature, u2: PoliticalFeature) = {
      assert(u1.value == u2.value)
      PoliticalFeature(u1.value, combine_maps(u1.spellings, u2.spellings),
        u1.num_accounts + u2.num_accounts,
        u1.num_refs + u2.num_refs,
        u1.num_lib_accounts + u2.num_lib_accounts,
        u1.num_lib_refs + u2.num_lib_refs,
        u1.num_cons_accounts + u2.num_cons_accounts,
        u1.num_cons_refs + u2.num_cons_refs,
        u1.num_refs_ideo_weighted + u2.num_refs_ideo_weighted,
        u1.all_text ++ u2.all_text)
    }
  }

  implicit val political_feature =
    mkCaseWireFormat(PoliticalFeature.apply _, PoliticalFeature.unapply _)

  class FindPoliticalDriver(opts: FindPoliticalParams)
      extends FindPoliticalAction(opts) {
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
     2. We go through users looking for references to these politicians.  For
        users that reference politicians, we can compute an "ideology" score of
        the user by a weighted average of the references by the ideology of
        the politicians.
     3. For each such user, look at all other people referenced -- the idea is
        we want to look for people referenced a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were referenced and an ideology score
        and merge these all together.
     */
  }

  def create_params(ap: ArgParser) = new FindPoliticalParams(ap)
  val progname = "FindPolitical"

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
     2. We go through users looking for references to these politicians.  For
        users that reference politicians, we can compute an "ideology" score of
        the user by a weighted average of the references by the ideology of
        the politicians.
     3. For each such user, look at all other people referenced -- the idea is
        we want to look for people referenced a lot especially by users with
        a consistent ideology (e.g. Glenn Beck or Rush Limbaugh for
        conservatives), which we can then use to mark others as having a
        given ideology.  For each person, we generate a record with their
        name, the number of times they were referenced and an ideology score
        and merge these all together.
     */
    val ptp = new FindPoliticalDriver(opts)
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
    val suffix = "tweets"
    val in_schema_file =
      CorpusFileProcessor.find_schema_file(filehand, opts.input, suffix)
    opts.schema = Schema.read_schema_file(filehand, in_schema_file)

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
    val ideo_fact = new IdeologicalUserAction(opts)
    val matching_patterns = CorpusFileProcessor.
        get_matching_patterns(filehand, opts.input, suffix)
    val lines: DList[String] = TextInput.fromTextFile(matching_patterns: _*)
    val ideo_users =
      lines.flatMap(ideo_fact.get_ideological_user(_, accounts))
    output_lines(ideo_users.map(_.to_row(opts)), "ideo-users",
      ideo_fact.row_fields)
    errprint("Step 1: done.")

    /* This is a separate function because including it inline in the for loop
       below results in a weird deserialization error. */
    def handle_political_feature_type(ty: String) {
      errprint("Step 2: Handling feature type '%s' ..." format ty)
      val political_features = ideo_users.
        flatMap(PoliticalFeature.
          get_political_features(ideo_fact, _, ty, opts)).
        groupBy(_.value).
        combine(PoliticalFeature.merge_political_features).
        map(_._2)
      output_lines(political_features.map(_.to_row(opts)),
        "political-features-%s" format ty, PoliticalFeature.row_fields)
    }

    errprint("Step 2: Generate political features.")
    for (ty <- opts.political_feature_type)
      handle_political_feature_type(ty)
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

