///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Ben Wing, The University of Texas at Austin
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
package opennlp.textgrounder.ners;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import java.io.IOException;
import java.lang.ClassNotFoundException;

import edu.stanford.nlp.ie.crf.CRFClassifier;

import opennlp.textgrounder.textstructs.CorpusDocument;
import opennlp.textgrounder.textstructs.Token;
import opennlp.textgrounder.textstructs.old.Lexicon;
import opennlp.textgrounder.util.Constants;
import opennlp.textgrounder.util.StringUtil;

/**
 * Named entity recognition using the Stanford NER.
 */
public class StanfordNER extends NamedEntityRecognizer {
    
    /**
     * NER system. This uses the Stanford NER system {@link TO COME}.
     */
    public static CRFClassifier classifier;

    /**
     * Default constructor.
     */
    public static void createClassifier() {
        if (classifier == null) {
            Properties myClassifierProperties = new Properties();
            classifier = new CRFClassifier(myClassifierProperties);
            try {
                classifier
                        .loadClassifier(Constants.STANFORD_NER_HOME
                                + "/classifiers/ner-eng-ie.crf-3-all2008-distsim.ser.gz");
            }
            /*
             * Checked exceptions are an utterly bogus concept. Just convert
             * them all to unchecked exceptions.
             */
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Split `text' into tokens (potentially multi-word). Determine whether each
     * token is a toponym and whether it's a stopword (using `stopwordList').
     * Add each token along with its properties (e.g. whether toponym, stopword,
     * etc.) to `doc', which stores a sequential list of all tokens in the
     * document.
     * 
     * FIXME: This code is specific to the Stanford NER. Code to interface to
     * different NER's should be abstracted into separate classes.
     * 
     * First, the entire input string in `text' is passed to the NER classifier.
     * This splits the text into tokens and determines which ones are toponyms.
     * The output of the classifier will be of the form
     * <p>
     * token/LABEL token/LABELpunctuation/LABEL token/LABEL ...
     * </p>
     * Here's an example:
     * <p>
     * I/O complained/O to/O Microsoft/ORGANIZATION about/O Bill/PERSON
     * Gates/PERSON ./O They/O told/O me/O to/O see/O the/O mayor/O of/O
     * New/LOCATION York/LOCATION ./O
     * </p>
     * 
     * FIXME: The code below, and the first example above, indicates that
     * punctuation is joined to a preceding token without whitespace. The second
     * example (from the Stanford NER FAQ at
     * `http://nlp.stanford.edu/software/crf-faq.shtml') indicates otherwise.
     * What's really going on?
     * 
     * If LABEL is `LOCATION', it is processed as a toponym or as a multiword
     * toponym if necessary. Any word that is not identified as a toponym is
     * processed as a regular token. Note that multi-word locations are output
     * as separate tokens (e.g. "New York" in the above example), but we combine
     * them into a single token.
     * 
     * When adding tokens and toponyms, all LABELs and the preceding slashes are
     * removed and the token is lowercased before being added to the lexicon and
     * to the document.
     * 
     * @param doc
     *            Document object corresponding to the current document.
     * @param text
     *            StringBuffer of input to be processed. This should always be
     *            space delimited raw text.
     */
    public void processText(CorpusDocument doc, String text) {

        Lexicon lexicon = doc.corpus.lexicon;
        
        createClassifier();
        
        /* Get NER output */
        String nerOutput = classifier.classifyToString(text);

        /*
         * Split NER output into space-separated "tokens" (may not correspond
         * with the tokens we store into the document, see above)
         */
        String[] tokens = nerOutput.replaceAll("-[LR]RB-", "").split(" ");
        int toponymStartIndex = -1;
        int toponymEndIndex = -1;

        /* For each NER "token", process it */
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            int wordidx = 0;
            boolean isLocationToken = token.contains("/LOCATION");
            if (isLocationToken) {
                /*
                 * We've seen a location token. We have to deal with multi-word
                 * location tokens, so if we see a location token, don't add it
                 * yet to the document, just remember the position
                 */
                if (toponymStartIndex == -1) {
                    toponymStartIndex = i;
                }
                /*
                 * Handle location tokens ending in /O (?)
                 * 
                 * FIXME: When does this happen? Does the output really include
                 * tokens like `foo/LOCATION/O' ?
                 */
                if (token.endsWith("/O")) {
                    toponymEndIndex = i + 1;
                    String cur = StringUtil.join(tokens, " ",
                            toponymStartIndex, toponymEndIndex, "/")
                            .toLowerCase();
                    wordidx = lexicon.addWord(cur);
                    doc.add(new Token(doc, wordidx, true));

                    toponymStartIndex = -1;
                    toponymEndIndex = -1;
                }
            } else if (toponymStartIndex != -1) {
                /*
                 * We've seen a non-location token, and there was a location
                 * token before us. Add both the location (potentially
                 * multi-word) and the current non-location token.
                 */
                toponymEndIndex = i;
                /**
                 * Add the toponym that spans from toponymStartIndex to
                 * toponymEndIndex. This does not include the current token
                 */
                String cur = StringUtil.join(tokens, " ", toponymStartIndex,
                        toponymEndIndex, "/").toLowerCase();
                wordidx = lexicon.addWord(cur);
                doc.add(new Token(doc, wordidx, true));

                /**
                 * Add the current token.
                 * 
                 * According to the code below, multiple non-word tokens can be
                 * joined together without spaces, e.g. 'here/O/./O'.
                 * 
                 * See comment above.
                 */
                List<String> subtokes = retrieveWords(token);
                for (String subtoke : subtokes) {
                    if (!subtoke.isEmpty()) {
                        subtoke = subtoke.trim().toLowerCase();
                        wordidx = lexicon.addWord(subtoke);
                        doc.add(new Token(doc, wordidx, false));
                    }
                }

                toponymStartIndex = -1;
                toponymEndIndex = -1;
            } else {
                /*
                 * A non-location token, no location token preceding. Just add
                 * it.
                 * 
                 * FIXME: Duplicated code.
                 */
                List<String> subtokes = retrieveWords(token);
                for (String subtoke : subtokes) {
                    if (!subtoke.isEmpty()) {
                        subtoke = subtoke.trim().toLowerCase();
                        wordidx = lexicon.addWord(subtoke);
                        doc.add(new Token(doc, wordidx, false));
                    }
                }
            }
        }

        // case where toponym ended at very end of document:
        if (toponymStartIndex != -1) {
            int wordidx = lexicon.addWord(StringUtil.join(tokens, " ", toponymStartIndex, toponymEndIndex, "/"));
            doc.add(new Token(doc, wordidx, true));
        }
    }

    /**
     * Find the non-label tokens in a single string with multiple tokens that
     * does not have whitespace boundaries.
     * 
     * FIXME: Duplicated functionality with two-arg version of function below.
     * 
     * @param token
     * @return
     */
    protected List<String> retrieveWords(String token) {
        String[] sarray = token.split("(/LOCATION|/PERSON|/ORGANIZATION|/O)");
        List<String> valid_tokens = new ArrayList<String>();
        for (String cur : sarray) {
            if (!cur.matches("^\\W*$")) {
                valid_tokens.add(cur);
            }
        }
        return valid_tokens;
    }

    /**
     * Retrieve first string of alphanumeric elements in an array of strings.
     *
     * The Stanford NER will add multiple /O, /LOCATION, /PERSON etc. tags to
     * a single token if it includes punctuation (though there might be other
     * character types that trigger the NER system to add multiple tags). We
     * take this token with multiple tags and split it on the slashes. This
     * method will find the first string that includes only characters
     * that fit the regex <p>^\\W*$</p>. If no such string is found, it
     * returns the empty string "".
     *
     * @param sarray Array of strings.
     * @param idx Index value in array of strings to examine
     * @return String that has only alphanumeric characters
     */
    protected String retrieveWords(String[] sarray, int idx) {
        String cur = null;
        try {
            cur = sarray[idx];
        } catch (ArrayIndexOutOfBoundsException e) {
            return "";
        }

        if (idx != 0 && cur.startsWith("O")) {
            cur = cur.substring(1);
        }

        if (!cur.matches("^\\W*$")) {
            return cur;
        } else {
            return retrieveWords(sarray, idx + 1);
        }
    }
}
