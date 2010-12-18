#!/usr/bin/python

import os
from nlputil import *

# Run a series of disambiguation experiments.

if 'TEXTGROUNDER_PYTHON' in os.environ:
  tgdir = os.environ['TEXTGROUNDER_PYTHON']
else:
  tgdir = '%s/python' % (os.environ['TEXTGROUNDER_DIR'])

def runit(fun, id, args):
  command='%s --id %s documents %s' % (Opts.run_cmd, id, args)
  errprint("Executing: %s" % command)
  if not Opts.dry_run:
    os.system("%s" % command)

def combine(*funs):
  def do_combine(fun, *args):
    for f in funs:
      f(fun, *args)
  return do_combine

def iterate(paramname, vals):
  def do_iterate(fun, id, args):
    for val in vals:
      fun('%s.%s' % (id, val), '%s %s %s' % (args, paramname, val))
  return do_iterate

def add_param(param):
  def do_add_param(fun, id, args):
    fun(id, '%s %s' % (args, param))
  return do_add_param

def recurse(funs, *args):
  if not funs:
    return
  (funs[0])(lambda *args: recurse(funs[1:], *args), *args)

def nest(*nest_funs):
  def do_nest(fun, *args):
    recurse(nest_funs + (fun,), *args)
  return do_nest

def run_exper(exper, expername):
  exper(lambda fun, *args: runit(id, *args), expername, '')

def main():
  op = OptionParser(usage="%prog [options] experiment [...]")
  op.add_option("-n", "--dry-run", action="store_true",
		  help="Don't execute anything; just output the commands that would be executed.")
  def_runcmd = '%s/run-run-disambig' % tgdir
  op.add_option("-c", "--run-cmd", "--cmd", default=def_runcmd,
		  help="Command to execute; default '%default'.")
  (opts, args) = op.parse_args()
  global Opts
  Opts = opts
  if not args:
    op.print_help()
  for exper in args:
    run_exper(eval(exper), exper)

##############################################################################
#                       Description of experiments                           #
##############################################################################

MTS300 = iterate('--max-time-per-stage', [300])
Train200k = iterate('--num-training-docs', [200000])
Train100k = iterate('--num-training-docs', [100000])
Test1k = iterate('--num-test-docs', [1000])
Test500 = iterate('--num-test-docs', [500])
#CombinedNonBaselineStrategies = add_param('--strategy partial-kl-divergence --strategy cosine-similarity --strategy naive-bayes-with-baseline --strategy per-word-region-distribution')
CombinedNonBaselineStrategies = add_param('--strategy partial-kl-divergence --strategy smoothed-cosine-similarity --strategy naive-bayes-with-baseline --strategy per-word-region-distribution')
CombinedNonBaselineNoCosineStrategies = add_param('--strategy partial-kl-divergence --strategy naive-bayes-with-baseline --strategy per-word-region-distribution')
NonBaselineStrategies = iterate('--strategy',
    ['partial-kl-divergence', 'per-word-region-distribution', 'naive-bayes-with-baseline', 'smoothed-cosine-similarity'])
BaselineStrategies = iterate('--strategy baseline --baseline-strategy',
    ['link-most-common-toponym', 'regdist-most-common-toponym',
    'internal-link', 'num-articles', 'random'])
CombinedBaselineStrategies1 = add_param('--strategy baseline --baseline-strategy link-most-common-toponym --baseline-strategy regdist-most-common-toponym')
CombinedBaselineStrategies2 = add_param('--strategy baseline --baseline-strategy internal-link --baseline-strategy num-articles --baseline-strategy random')
CombinedBaselineStrategies = combine(CombinedBaselineStrategies1, CombinedBaselineStrategies2)
AllStrategies = combine(NonBaselineStrategies, BaselineStrategies)
CombinedKL = add_param('--strategy symmetric-partial-kl-divergence --strategy symmetric-kl-divergence --strategy partial-kl-divergence --strategy kl-divergence')
CombinedCosine = add_param('--strategy cosine-similarity --strategy smoothed-cosine-similarity --strategy partial-cosine-similarity --strategy smoothed-partial-cosine-similarity')
KLDivStrategy = iterate('--strategy', ['partial-kl-divergence'])
FullKLDivStrategy = iterate('--strategy', ['kl-divergence'])
SmoothedCosineStrategy = iterate('--strategy', ['smoothed-cosine-similarity'])
NBStrategy = iterate('--strategy', ['naive-bayes-no-baseline'])

Coarser1DPR = iterate('--degrees-per-region', [0.1, 10])
Coarser2DPR = iterate('--degrees-per-region', [0.5, 1, 5])
CoarseDPR = iterate('--degrees-per-region',
    #[90, 30, 10, 5, 3, 2, 1, 0.5]
    #[0.5, 1, 2, 3, 5, 10, 30, 90]
    [0.5, 1, 2, 3, 5, 10])
OldFineDPR = iterate('--degrees-per-region',
    [90, 75, 60, 50, 40, 30, 25, 20, 15, 12, 10, 9, 8, 7, 6, 5, 4, 3, 2.5, 2,
     1.75, 1.5, 1.25, 1, 0.87, 0.75, 0.63, 0.5, 0.4, 0.3, 0.25, 0.2, 0.15, 0.1]
    )
DPR3 = iterate('--degrees-per-region', [3])
DPR5 = iterate('--degrees-per-region', [5])

DPR1 = iterate('--degrees-per-region', [0.5, 1, 3])
DPRpoint5 = iterate('--degrees-per-region', [0.5])

MinWordCount = iterate('--minimum-word-count', [1, 2, 3, 4, 5])

CoarseDisambig = nest(MTS300, AllStrategies, CoarseDPR)

# PCL experiments
PCLDPR = iterate('--degrees-per-region', [1.5, 0.5, 1, 2, 3, 5])
PCLEvalFile = add_param('-f pcl-travel -e /groups/corpora/pcl_travel/books')
PCLDisambig = nest(MTS300, PCLEvalFile, NonBaselineStrategies, PCLDPR)

# Param experiments

ParamExper = nest(MTS300, DPR1, MinWordCount, NonBaselineStrategies)

# Fine experiments

FinerDPR = iterate('--degrees-per-region', [0.3, 0.2, 0.1])
EvenFinerDPR = iterate('--degrees-per-region', [0.1, 0.05])
Finer3DPR = iterate('--degrees-per-region', [0.01, 0.05])
FinerExper = nest(MTS300, FinerDPR, KLDivStrategy)
EvenFinerExper = nest(MTS300, EvenFinerDPR, KLDivStrategy)

# Missing experiments

MissingNonBaselineStrategies = iterate('--strategy',
    ['naive-bayes-no-baseline', 'partial-cosine-similarity', 'cosine-similarity'])
MissingBaselineStrategies = iterate('--strategy baseline --baseline-strategy',
    ['link-most-common-toponym'
      #, 'regdist-most-common-toponym'
      ])
MissingOtherNonBaselineStrategies = iterate('--strategy',
    ['partial-cosine-similarity', 'cosine-similarity'])
MissingAllButNBStrategies = combine(MissingOtherNonBaselineStrategies,
    MissingBaselineStrategies)
#Original MissingExper failed on or didn't include all but
#regdist-most-common-toponym.
#MissingExper = nest(MTS300, CoarseDPR, MissingAllStrategies)

MissingNBExper = nest(MTS300, CoarseDPR, NBStrategy)
MissingOtherExper = nest(MTS300, CoarseDPR, MissingAllButNBStrategies)
MissingBaselineExper = nest(MTS300, CoarseDPR, MissingBaselineStrategies)
FullKLDivExper = nest(MTS300, CoarseDPR, FullKLDivStrategy)

# Newer experiments on 200k/1k

#CombinedKLExper = nest(Train100k, Test1k, DPR5, CombinedKL)
#CombinedCosineExper = nest(Train100k, Test1k, DPR5, CombinedCosine)
CombinedKLExper = nest(Train100k, Test500, DPR5, CombinedKL)
CombinedCosineExper = nest(Train100k, Test500, DPR5, CombinedCosine)

NewCoarser1Exper = nest(Train100k, Test500, Coarser1DPR, CombinedNonBaselineStrategies)
NewCoarser2Exper = nest(Train100k, Test500, Coarser2DPR, CombinedNonBaselineStrategies)
NewFiner3Exper = nest(Train100k, Test500, Finer3DPR, KLDivStrategy)
NewIndiv4Exper = nest(Train100k, Test500, DPRpoint5, CombinedNonBaselineNoCosineStrategies)
NewIndiv5Exper = nest(Train100k, Test500, DPRpoint5, CombinedBaselineStrategies1)

NewDPR = iterate('--degrees-per-region', [0.1, 0.5, 1, 5])
NewDPR2 = iterate('--degrees-per-region', [0.1, 0.5, 1, 5, 10])
New10DPR = iterate('--degrees-per-region', [10])
New510DPR = iterate('--degrees-per-region', [5, 10])
New1DPR = iterate('--degrees-per-region', [1])
NewSmoothedCosineExper = nest(Train100k, Test500, SmoothedCosineStrategy, NewDPR)
NewSmoothedCosineExper2 = nest(Train100k, Test500, SmoothedCosineStrategy, New10DPR)
New10Exper = nest(Train100k, Test500, New10DPR, CombinedNonBaselineStrategies)
NewBaselineExper = nest(Train100k, Test500, NewDPR2, CombinedBaselineStrategies)
NewBaseline2Exper1 = nest(Train100k, Test500, New1DPR, CombinedBaselineStrategies2)
NewBaseline2Exper2 = nest(Train100k, Test500, New510DPR, CombinedBaselineStrategies)

TestDPR = iterate('--degrees-per-region', [0.1])
TestSet = add_param('--eval-set test')
TestStrat1 = iterate('--strategy', ['partial-kl-divergence'])
TestStrat2 = iterate('--strategy', ['per-word-region-distribution'])
TestStrat3 = iterate('--strategy', ['naive-bayes-with-baseline'])
TestExper1 = nest(Train100k, Test1k, TestSet, TestDPR, TestStrat1)
TestExper2 = nest(Train100k, Test1k, TestSet, TestDPR, TestStrat2)
TestExper3 = nest(Train100k, Test1k, TestSet, TestDPR, TestStrat3)

TestStratBase1 = add_param('--strategy baseline --baseline-strategy link-most-common-toponym --baseline-strategy regdist-most-common-toponym')
TestStratBase2 = add_param('--strategy baseline --baseline-strategy num-articles --baseline-strategy random')
TestExperBase1 = nest(Train100k, Test1k, TestSet, TestDPR, TestStratBase1)
TestExperBase2 = nest(Train100k, Test1k, TestSet, TestDPR, TestStratBase2)



TwitterDPR1 = iterate('--degrees-per-region', [0.5, 1, 0.1, 5, 10])
TwitterExper1 = nest(TwitterDPR1, KLDivStrategy)
TwitterDPR2 = iterate('--degrees-per-region', [1, 5, 10, 0.5, 0.1])
TwitterStrategy2 = add_param('--strategy naive-bayes-with-baseline --strategy smoothed-cosine-similarity --strategy per-word-region-distribution')
TwitterExper2 = nest(TwitterDPR2, TwitterStrategy2)
TwitterDPR3 = iterate('--degrees-per-region', [5, 10, 1, 0.5, 0.1])
TwitterStrategy3 = add_param('--strategy cosine-similarity')
TwitterExper3 = nest(TwitterDPR3, TwitterStrategy3)
TwitterBaselineExper1 = nest(TwitterDPR3, BaselineStrategies)

TwitterWikiNumTest = iterate('--num-test-docs', [1894])
TwitterWikiDPR1 = iterate('--degrees-per-region', [0.1])
TwitterWikiStrategyAll = add_param('--strategy partial-kl-divergence --strategy naive-bayes-with-baseline --strategy smoothed-cosine-similarity --strategy per-word-region-distribution')
TwitterWikiDPR2 = iterate('--degrees-per-region', [0.5])
TwitterWikiDPR3 = iterate('--degrees-per-region', [5, 10, 1])
TwitterWikiStrategy3 = add_param('--strategy partial-kl-divergence')
TwitterWikiDPR4 = iterate('--degrees-per-region', [5, 10, 1])
TwitterWikiStrategy4 = add_param('--strategy naive-bayes-with-baseline --strategy smoothed-cosine-similarity --strategy per-word-region-distribution')
TwitterWikiExper1 = nest(Train100k, TwitterWikiNumTest, TwitterWikiDPR1, TwitterWikiStrategyAll)
TwitterWikiExper2 = nest(Train100k, TwitterWikiNumTest, TwitterWikiDPR2, TwitterWikiStrategyAll)
TwitterWikiExper3 = nest(Train100k, TwitterWikiNumTest, TwitterWikiDPR3, TwitterWikiStrategy3)
TwitterWikiExper4 = nest(Train100k, TwitterWikiNumTest, TwitterWikiDPR4, TwitterWikiStrategy4)

# Test 

main()
