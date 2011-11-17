#!/usr/bin/python

import xml.dom.minidom as md
from nlputil import *
from itertools import *

# Find XML nodes using depth-first search.  Specify either 'name',
# which matches on a node with that name, or 'matches', a predicate to
# return a match.
def find_node_dfs(node, name=None, matches=None):
  if name is not None:
    def matches(node):
      return node.localName == name
  def children(node):
    return node.childNodes
  return depth_first_search(node, matches, children)

# Find XML nodes using breadth-first search.  Specify either 'name',
# which matches on a node with that name, or 'matches', a predicate to
# return a match.
def find_node_bfs(node, name=None, matches=None):
  if name is not None:
    def matches(node):
      return node.localName == name
  def children(node):
    return node.childNodes
  return breadth_first_search(node, matches, children)

foo = md.parse('/groups/corpora/pcl_travel/books/txu-oclc-9925892.xml')

def is_div_chapter(x):
  print "Saw x: %s" % x
  print "Saw attributes: %s" % x.attributes
  return x.localName == 'div' and x.getAttribute('type') == 'chapter'

def is_text_node(x):
  return x.nodeType == md.Attr.TEXT_NODE

for chapter in find_node_dfs(foo, matches=is_div_chapter):
  text = ''.join(t.data for t in find_node_dfs(chapter, matches=is_text_node))
  uniprint("Saw chapter text: %s" % (' '.join(split_text_into_words(text, include_nl=True))))
