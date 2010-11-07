#!/usr/bin/env python

# Any Python NER component should provide a function named "recognize" that
# takes a list of string tokens and returns a list of tuples indicating the
# beginning and ending of named entity ranges in the token list. This simple
# example considers all capitalized words to be single-token named entities.
def recognize(tokens):
  spans = []
  for i in range(0, len(tokens)):
    if tokens[i].isalnum() and tokens[i] == tokens[i].capitalize():
      spans.append((i, i + 1))
  return spans

#text = ["They", "had", "sailed", "from", "Deptford",
#        ",", "from", "Greenwich", ",", "from", "Erith", "..."]

#print(recognize(text))

