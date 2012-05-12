///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.tr.text.prep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import opennlp.textgrounder.tr.topo.gaz.*;
import opennlp.textgrounder.tr.util.*;

/**
 * Wraps a document source, removes any toponym spans, and identifies toponyms
 * using a named entity recognizer and a gazetteer.
 *
 * @author Travis Brown <travis.brown@mail.utexas.edu>
 * @version 0.1.0
 */
public class ToponymAnnotator extends DocumentSourceWrapper {
  private final NamedEntityRecognizer recognizer;
  private final Gazetteer gazetteer;
  private final Region boundingBox;

  public ToponymAnnotator(DocumentSource source,
                          NamedEntityRecognizer recognizer,
                          Gazetteer gazetteer) {
      this(source, recognizer, gazetteer, null);
  }

  public ToponymAnnotator(DocumentSource source,
                          NamedEntityRecognizer recognizer,
                          Gazetteer gazetteer,
                          Region boundingBox) {
    super(source);
    this.recognizer = recognizer;
    this.gazetteer = gazetteer;
    this.boundingBox = boundingBox;
  }

  public Document<Token> next() {
    final Document<Token> document = this.getSource().next();
    final Iterator<Sentence<Token>> sentences = document.iterator();

    return new Document<Token>(document.getId(), document.getTimestamp(), document.getGoldCoord(), document.getSystemCoord(), document.getSection(), document.title) {
      private static final long serialVersionUID = 42L;
      public Iterator<Sentence<Token>> iterator() {
        return new SentenceIterator() {
          public boolean hasNext() {
            return sentences.hasNext();
          }

          public Sentence<Token> next() {
            Sentence<Token> sentence = sentences.next();
            List<String> forms = new ArrayList<String>();
            List<Token> tokens = sentence.getTokens();

            for (Token token : tokens) {
              forms.add(token.getOrigForm());
            }

            List<Span<NamedEntityType>> spans = ToponymAnnotator.this.recognizer.recognize(forms);
            List<Span<Toponym>> toponymSpans = new ArrayList<Span<Toponym>>();

            for (Span<NamedEntityType> span : spans) {
              if (span.getItem() == NamedEntityType.LOCATION) {
                StringBuilder builder = new StringBuilder();
                for (int i = span.getStart(); i < span.getEnd(); i++) {
                  builder.append(forms.get(i));
                  if (i < span.getEnd() - 1) {
                    builder.append(" ");
                  }
                }

                String form = builder.toString();
                //List<Location> candidates = ToponymAnnotator.this.gazetteer.lookup(form.toLowerCase());
                List<Location> candidates = TopoUtil.filter(
                    ToponymAnnotator.this.gazetteer.lookup(form.toLowerCase()), boundingBox);
                if(candidates != null) {
                    for(Location loc : candidates) {
                        List<Coordinate> reps = loc.getRegion().getRepresentatives();
                        int prevSize = reps.size();
                        Coordinate.removeNaNs(reps);
                        if(reps.size() < prevSize)
                            loc.getRegion().setCenter(Coordinate.centroid(reps));
                    }
                }
                //if(form.equalsIgnoreCase("united states"))
                //    for(Location loc : ToponymAnnotator.this.gazetteer.lookup("united states"))
                //        System.out.println(loc.getRegion().getCenter());
                if (candidates != null) {
                  Toponym toponym = new SimpleToponym(form, candidates);
                  //if(form.equalsIgnoreCase("united states"))
                  //    System.out.println(toponym.getCandidates().get(0).getRegion().getCenter());
                  toponymSpans.add(new Span<Toponym>(span.getStart(), span.getEnd(), toponym));
                }
              }
            }

            return new SimpleSentence(sentence.getId(), tokens, toponymSpans);
          }
        };
      }
    };
  }
}

