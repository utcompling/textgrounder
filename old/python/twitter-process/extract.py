#!/usr/bin/python

from __future__ import with_statement
from odict import odict
from pprint import pprint
from collections import defaultdict
import os,sys,re,codecs
import twokenize,emoticons

if "<fdopen>" not in str(sys.stdout):
  sys.stdout = os.fdopen(1,'w',0)

opts = {
  'datafile': None,
  'word_count_thresh': 0,
  'doc_count_thresh': 2,
}

if len(sys.argv)>0:
  opts['datafile'] = sys.argv[1]

print "OPTIONS"
pprint(opts)

class Numberizer(odict):
  def add(self, ob):
    if ob in self: return
    self[ob] = len(self) + 1

users = Numberizer()
user_latlong = {}

vocab = Numberizer()
word_counts = defaultdict(int)
doc_counts = defaultdict(int)


Num = re.compile(r'^\d+$', re.U)
Url = re.compile('^' + twokenize.Url + '$', re.I|re.U)

def canonicalize_rare_word(w):
  repl = w
  repl = Num.sub("-NUM-",repl)
  repl = Url.sub("-URL-",repl)
  if repl != "-NUM-" and repl != "-URL-":
    repl = "-OOV-"
  return repl
  
def prune(vocab, word_counts, doc_counts, word_count_thresh, doc_count_thresh, **crap):
  replacements = {}
  for w in vocab:
    if word_counts[w] < word_count_thresh or doc_counts[w] < doc_count_thresh:
      replacements[w] = canonicalize_rare_word(w)
  print "%s -> %s vocab size" % (len(vocab), len(vocab)-len(replacements))
  
  # have to rejigger the vocab and counts
  new_vocab = Numberizer()
  new_word_counts = defaultdict(int)
  new_doc_counts = defaultdict(int)
  
  for w in vocab:
    r = replacements.get(w,w)
    new_vocab.add(r)
    new_word_counts[r] += word_counts[w]
    new_doc_counts[r] += doc_counts[w]
  return new_vocab, new_word_counts, new_doc_counts, replacements
  
  

cur_user_words = []
last_username = None

Punct = twokenize.regex_or(twokenize.PunctChars, twokenize.Entity, twokenize.EdgePunct, r'[\*]')
Punct_RE = re.compile('^(%s)+$' % Punct, re.I|re.U)

def get_tokens(text):
  toks = twokenize.tokenize(text.lower())
  toks = [t for t in toks if not t.startswith('@') and t != 'rt' ]
  toks = ["-PUNCT-" if Punct_RE.search(t) and not emoticons.Emoticon_RE.search(t) else t for t in toks]
  # toks = [t.replace("#","") for t in toks]
  return toks

# for line in sys.stdin:
#   # print get_tokens(line)
#   print (" ".join(get_tokens(line))).encode('utf-8')
#   # print "\n" + "\n".join(get_tokens(line))
# sys.exit(0)


def save_word_docs(filename):
  with codecs.open(filename,'w','utf-8') as f:
    for w in word_docs:
      my_counts = word_docs[w].items()
      my_counts.sort()
      my_counts = ["%s:%d" % pair for pair in my_counts]
      my_counts = " ".join(my_counts)
      print>>f, "%s\t%s" % (w, my_counts)


## pass 1: user info and word counting

# inverted index
word_docs = defaultdict(lambda: defaultdict(int))

for line in os.popen("cat "+opts['datafile']):
  parts = line[:-1].split("\t")
  username,dt, _latlong, lat,long,text = parts
  for w in get_tokens(text):
    vocab.add(w)
    word_counts[w] += 1
    word_docs[w][username] += 1

# save_word_docs("word_docs")

for w in word_docs:
  doc_counts[w] = len(word_docs[w])

def save_vocab_info(filename):
  with codecs.open(filename,'w','utf-8') as f:
    for word, word_num in vocab.iteritems():
      print>>f, "%s\t%s\t%s" % (word, word_counts[word], doc_counts[word])

if not os.path.exists("vocab_unpruned"):
  save_vocab_info("vocab_unpruned")

## word count cutoff analysis
vocab,word_counts,doc_counts,replacements = prune(vocab,word_counts,doc_counts, **opts)
print "new vocab ", len(vocab)
save_vocab_info("vocab_wc_dc")


###### pass 2: print numberized text

def flush_cur_user():
  global cur_user_words
  if not last_username: return
  for n,w in enumerate(cur_user_words):
    if w in replacements: w = replacements[w]
    print>>user_pos_word_sym, w.encode('utf-8')
    print>>user_pos_word, "%s\t%s\t%s" % (users[last_username], n+1, vocab[w])
  cur_user_words = []


cur_user_words = []
last_username = None
user_pos_word = open("user_pos_word",'w')
user_pos_word_sym = open("user_pos_word_sym",'w')
# user_pos_word = sys.stdout

for line in os.popen("cat "+opts['datafile']):
  parts = line[:-1].split("\t")
  username,dt, _latlong, lat,long,text = parts
  users.add(username)
  
  if username != last_username:
    flush_cur_user()
    last_username = username
  
  if username not in user_latlong:
    user_latlong[username] = (lat,long)
  
  cur_user_words += get_tokens(text)
flush_cur_user()

with open("user_info",'w') as f:
  for username,user_num in users.iteritems():
    print>>f, "\t".join([username, user_latlong[username][0], user_latlong[username][1]])


