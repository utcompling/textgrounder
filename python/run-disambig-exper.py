#!/usr/bin/python

import os
from nlputil import *

# Run a series of disambiguation experiments.

tgdir = os.environ['TEXTGROUNDER_DIR']
runcmd = '%s/python/run-run-disambig' % tgdir
#common_args='--max-time-per-stage 5'
common_args='--max-time-per-stage 300'

degrees_per_region=[90, 30, 10, 5, 3, 2, 1, 0.5]
strategy=['partial-kl-divergence', 'per-word-region-distribution', 'baseline']
baseline_strategy=['internal-link', 'random', 'num-articles',
    'region-distribution-most-common-proper-noun']

def runit(id, args):
  command='%s documents --id %s %s %s' % (runcmd, id, common_args, args)
  errprint("Executing: %s" % command)
  os.system("%s" % command)

def iter_strategy(fun, id, args):
  for strat in strategy:
    if strat == 'baseline':
      for basestrat in baseline_strategy:
        fun('%s.%s' % (id, basestrat),
            '%s --strategy baseline --baseline-strategy %s' %
            (args, basestrat))
    else:
      fun('%s.%s' % (id, strat), '%s --strategy %s' % (args, strat))

def iter_dpr(fun, id, args):
  for dpr in degrees_per_region:
    fun('%s.%s' % (id, dpr), '%s --degrees-per-region %s' % (args, dpr))

def recurse(funs, *args):
  if not funs:
    return
  (funs[0])(lambda *args: recurse(funs[1:], *args), *args)

recurse([iter_strategy, iter_dpr, (lambda fun, id, args: runit(id[1:], args))],
    '', '')
