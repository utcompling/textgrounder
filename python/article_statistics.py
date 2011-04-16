#!/usr/bin/env python

#######
####### article_statistics.py
#######
####### Copyright (c) 2010 Ben Wing.
#######

# Counts and outputs various statistics about the articles in the
# article-data file.

from nlputil import *
import process_article_data as pad

######################## Statistics about articles

class ArticleStatistics(object):
  def __init__(self):
    self.total_num = 0
    self.num_by_split = intdict()
    self.num_redir = 0
    self.num_by_namespace = intdict()
    self.num_list_of = 0
    self.num_disambig = 0
    self.num_list = 0

  def record_article(self, art):
    self.total_num += 1
    self.num_by_split[art.split] += 1
    self.num_by_namespace[art.namespace] += 1
    if art.redir:
      self.num_redir += 1
    if art.is_list_of:
      self.num_list_of += 1
    if art.is_disambig:
      self.num_disambig += 1
    if art.is_list:
      self.num_list += 1

  def output_stats(self, stats_type, outfile=sys.stdout):
    def outprint(foo):
      uniprint(foo, outfile=outfile)
    outprint("Article statistics about %s" % stats_type)
    outprint("  Total number = %s" % self.total_num)
    outprint("  Number of redirects = %s" % self.num_redir)
    outprint("  Number of 'List of' articles = %s" % self.num_list_of)
    outprint("  Number of disambiguation pages = %s" % self.num_disambig)
    outprint("  Number of list articles = %s" % self.num_list)
    outprint("  Statistics by split:")
    output_reverse_sorted_table(self.num_by_split, indent="    ")
    outprint("  Statistics by namespace:")
    output_reverse_sorted_table(self.num_by_namespace, indent="    ")

class ArticleStatisticsSet(object):
  def __init__(self, set_name):
    self.set_name = set_name
    self.stats_all = ArticleStatistics()
    self.stats_redir = ArticleStatistics()
    self.stats_non_redir = ArticleStatistics()
    self.stats_list_of = ArticleStatistics()
    self.stats_disambig = ArticleStatistics()
    self.stats_list = ArticleStatistics()
    self.stats_non_list = ArticleStatistics()
    self.stats_by_split = {}
    self.stats_by_namespace = {}

  def record_article(self, art):
    def record_by_value(art, table, value):
      if value not in table:
        table[value] = ArticleStatistics()
      table[value].record_article(art)

    self.stats_all.record_article(art)
    if art.redir:
      self.stats_redir.record_article(art)
    else:
      self.stats_non_redir.record_article(art)
    if art.is_list_of:
      self.stats_list_of.record_article(art)
    if art.is_disambig:
      self.stats_disambig.record_article(art)
    if art.is_list:
      self.stats_list.record_article(art)
    else:
      self.stats_non_list.record_article(art)
    record_by_value(art, self.stats_by_split, art.split)
    record_by_value(art, self.stats_by_namespace, art.namespace)

  def output_stats(self):
    def out(stats, stats_type):
      stats.output_stats("%s (%s)" % (stats_type, self.set_name))
    out(self.stats_all, "all articles")
    out(self.stats_redir, "redirect articles")
    out(self.stats_non_redir, "non-redirect articles")
    out(self.stats_list_of, "'List of' articles")
    out(self.stats_disambig, "disambiguation pages")
    out(self.stats_list, "list articles")
    for (split, stat) in self.stats_by_split.iteritems():
      out(stat, "articles in %s split" % split)
    for (namespace, stat) in self.stats_by_namespace.iteritems():
      out(stat, "articles in namespace %s" % namespace)
    uniprint("")

Stats_set_all = ArticleStatisticsSet("all articles")
Stats_set_non_redir = ArticleStatisticsSet("non-redirect articles")
Stats_set_main_non_redir = ArticleStatisticsSet("non-redirect articles, namespace Main")
Stats_set_main_non_redir_non_list = ArticleStatisticsSet("non-redirect articles, namespace Main, non-list articles")

def note_article_for_global_stats(art):
  Stats_set_all.record_article(art)
  if not art.redir:
    Stats_set_non_redir.record_article(art)
    if art.namespace == 'Main':
      Stats_set_main_non_redir.record_article(art)
      if not art.is_list:
        Stats_set_main_non_redir_non_list.record_article(art)

def output_global_stats():
  Stats_set_all.output_stats()
  Stats_set_non_redir.output_stats()
  Stats_set_main_non_redir.output_stats()
  Stats_set_main_non_redir_non_list.output_stats()
  
def generate_article_stats(filename):
  def process(art):
    note_article_for_global_stats(art)
  pad.read_article_data_file(filename, process=process,
                             maxtime=Opts.max_time_per_stage)
  output_global_stats()

############################################################################
#                                  Main code                               #
############################################################################

class ArticleStatisticsProgram(NLPProgram):
  def argument_usage(self):
    return "article-data-file"

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    if len(args) != 1:
      op.error("Must specify exactly one article-data file as an argument")

  def implement_main(self, opts, params, args):
    generate_article_stats(args[0])
    
ArticleStatisticsProgram()
