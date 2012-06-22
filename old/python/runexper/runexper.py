#!/usr/bin/python

from ConfigParser import *
import re
import os
import time
from nlputil import *

# NOTE NOTE NOTE: This was an early attempt at writing an experiment-running
# front end.  Superseded by the simpler program run-geolocate-exper.py
# and by qsub-tg-geolocate for submitting multiple experiments using qsub.

# A more-better config parser.  Expands using $foo references instead of
# %(foo)s references.  References are looked up as follows:
#
# 1. If in the format 'fun:arg', it's a function call, which is expanded
#    dynamically.
# 2. An option 'foo' in the current section.
# 3. An option 'foo' in the [default] section.
# 4. If in the format 'bar.foo', an option 'foo' in section 'bar'.
# 5. An environment variable.
#
# When looking up an option in a section, references in that section are
# recursively expanded.
#
# The currently defined functions are:
#
# time: The time formatted using time.strftime().  If the argument is blank,
#       use the format %Y-%m-%d.%H%M%S, e.g. 2010-05-10.224852 for a time
#       10:48:22pm on May 10, 2010.

class InterpolatingConfigParser(RawConfigParser):
  def expand_funcall(self, section, fun, arg):
    fun = fun.lower()
    if fun == 'time':
      if not arg:
        arg = '%Y-%m-%d.%H%M%S'
      return time.strftime(arg)
    raise InterpolationSyntaxError('fun:arg' % (fun, arg), section,
        "Unknown function call: %s:%s" % (fun, arg))

  def lookup_ref(self, section, ref):
    if ':' in ref:
      m = re.match(r'(.*?):(.*)$', ref)
      return self.expand_funcall(section, m.group(1), m.group(2))
    if self.has_option(section, ref):
      return self.interpolate(section, self.get(section, ref))
    defs = self.defaults()
    if ref in defs:
      return self.interpolate('default', defs[ref])
    m = re.match(r'(.*)\.(.*)$', ref)
    if m:
      sec = m.group(1)
      secref = m.group(2)
      if self.has_option(sec, secref):
        return self.interpolate(sec, self.get(sec, secref))
    if ref in os.environ:
      return os.environ[ref]
    raise InterpolationSyntaxError(ref, section,
        "Unknown variable reference: %s" % ref)

  def interpolate(self, section, val):
    def interpolate_var(varref):
      print "interpolate_var called with %s" % varref
      if not varref or varref[0] != '$':
        return varref
      if varref[1] == '{' or varref[1] == '(':
        var = varref[2:-1]
      else:
        var = varref[1:]
      return self.lookup_ref(section, var)

    return (''.join(interpolate_var(varref) for varref in
            re.split(r'(\$(?:[a-zA-Z0-9_]+|\(.*?\)|\{.*?\}))', val)))

  def get(self, section, ref, defaults={}):
    if self.has_option(section, ref):
      val = RawConfigParser.get(self, section, ref)
    elif ref in defaults:
      val = defaults[ref]
    else:
      raise NoOptionError(ref, section)
    return self.interpolate(section, val)

  def items(self, section):
    return [(name, self.interpolate(section, value)) for name, value in
            RawConfigParser.items(self, section)]


############################################################################
#                                  Main code                               #
############################################################################

# Split a string on whitespace, but allow single or double quoted strings,
# similar to how the shell works.  Also substitute backslash sequences like
# \n or \x53.
def split_values(str):
  for val in re.findall(r'''(?:[^\s"']|"(?:[^\\"]|\\.)+"|'(?:[^\\']|\\.)+')+''',
                        str):
    if val and ((val[0] == '"' and val[-1] == '"') or
                (val[0] == "'" and val[-1] == "'")):
      val = val[1:-1]
    yield val.decode('string_escape')

class Experiment(object):
  def __init__(self, options, defaults={}):
    self.options = {}
    for (key, val) in defaults.iteritems():
      self.options[key] = val
    for (key, val) in options.iteritems():
      self.options[key] = val

  def __str__(self):
    return "Experiment(%s)" % self.options

class ExperimentConfigParser(InterpolatingConfigParser):
  def get_option(self, table, key, section):
    if key not in table:
      raise ExperimentConfigError("Expected option '%s' in section '%s'" %
                                  (key, section))
      return table[key]

  def get_options(self, section, defaults={}):
    opts = {}
    for (key, val) in defaults.iteritems():
      opts[key] = val
    for (key, val) in self.items(section):
      opts[key] = val
    return opts

  def yield_instances(self, section, defaults={}):
    def recursive_iterate(iterlist):
      if len(iterlist) 
      insts = list(yield_instances(self, 
      if iterlist

    opts = self.get_options(section, defaults)
    if 'iterate' in opts:
      recursive_iterate(split_values(opts['iterate'])):

      itersecs = 
    arg = self.get_option(opts, 'arg', section)
    vals = self.get_option(opts, 'values', section)
    for val in split_values(vals):
      yield Experiment({'expanded-arg':arg % val}, opts)

  def run_experiment(self, section, defaults={}):
    for inst in self.yield_instances(section, defaults):
      print "Saw instance %s" % inst

class ExperimentConfigError(StandardError):
  pass

def run_experiment(filename):
  config = ExperimentConfigParser()
  config.read([filename])
  config.run_experiment('experiment')

class ExperimentRunnerProgram(NLPProgram):
  def argument_usage(self):
    return "experiment-conf-file ..."

  def handle_arguments(self, opts, op, args):
    global Opts
    Opts = opts
    if len(args) < 1:
      op.error("Must specify at least one experiment config file")

  def implement_main(self, opts, params, args):
    for filename in args:
      run_experiment(filename)

ExperimentRunnerProgram()

## General experiment runner.
#config = InterpolatingConfigParser()
#val = config.read(['foo.conf'])
#print "config.read returned %s" % val
#
#secs = config.sections()
#print "Sections: %s" % secs
#for x in secs:
#  print "[%s]" % x
#  for (name, value) in config.items(x):
#    print "  [%s]=[%s]" % (name, value)
