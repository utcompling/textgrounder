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

import java.io.*;
import java.util.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.util.*;

/**
 * Named entity recognition using the OpenNLP NER.
 */
public class OpenNLPNER extends NamedEntityRecognizer {
    protected static final String sep = File.separator;
    public static final String sentence_model = Constants.OPENNLP_MODELS + sep + "english" + sep + "sentdetect" + sep + "EnglishSD.bin.gz"; 
    public static final String ner_model_dir = Constants.OPENNLP_MODELS + sep + "english" + sep + "namefind";
   
    /**
     * Default constructor.
     */
    public OpenNLPNER() {
        super();
        if (Constants.OPENNLP_MODELS.equals(""))
            throw new RuntimeException("Environment var OPENNLP_MODELS needs to be set to point to the\n" +
                    "location of the model data, necessary for the proper working of OpenNLP. (You need\n" +
                    "to download it separately from the OpenNLP source code.)");
    }

    /**
     * Perform sentence detection the given text. On input, an empty line will
     * be treated as a paragraph boundary. Returns a string with each sentence
     * on a separate line.
     */
    protected String runOpenNLPSentenceDetector(String text) {
        try {
//            System.out.println("Sentence detector called.");
            String result = OpenNLPSentenceDetector.run(sentence_model, text); 
//            System.out.println("Input:\n========cut========");
//            System.out.print(text);
//            System.out.println("========cut========");
//            System.out.println();
//            System.out.println("Output:\n========cut========");
//            System.out.print(result);
//            System.out.println("========cut========");
//            System.out.println();
            return result;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
  
    protected List<List<List<String[]>>> runOpenNLPNER(String text) {
        try {
            // Create the list of models -- everything in the models dir that
            // ends with .bin.gz.
            List<String> models = new ArrayList<String>();
            File modeldirfile = new File(ner_model_dir);
            if (!modeldirfile.isDirectory())
                throw new RuntimeException("Model dir `" + ner_model_dir
                        + "' (from env var OPENNLP_MODELS) is not a directory");
            for (String model : modeldirfile.list()) {
                if (model.matches(".*\\.bin\\.gz$")) {
                    models.add(modeldirfile.getCanonicalPath() + sep + model);
                }
            }
            if (models.isEmpty()) {
                throw new RuntimeException("??? No models in " + ner_model_dir);
            }

            // Process the text.
            return OpenNLPNameFinder.run(models, text);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Run the text through the OpenNLP named-entity recognizer (aka "name finder").
     * Using the results, populate the divisions and tokens in the CorpusDocument.
     */
    public void processText(CorpusDocument doc, String text) {
        for (List<List<String[]>> para : runOpenNLPNER(runOpenNLPSentenceDetector(text))) {
            // All of this code is interface code to convert the structure
            // returned by runOpenNLPNER to the structure expected by
            // CorpusDocument.
            Division paradiv = new Division(doc, "p");
            for (List<String[]> sent : para) {
                Division sentdiv = new Division(doc, "s");
                for (String[] token : sent) {
                    String tokstr = token[0];
                    String toktype = token[1];
                    String netype = null;
                    boolean istok = false;
                    int tokid = doc.corpus.lexicon.addWord(tokstr);

                    if (toktype == null) netype = "O";
                    else if (toktype.equals("person")) netype = "I-PER";
                    else if (toktype.equals("organization")) netype = "I-ORG";
                    else if (toktype.equals("location")) istok = true;
                    // "date", "time", "percentage", "money"
                    else netype = "I-MISC"; 

                    Token tok = new Token(doc, tokid, istok);
                    if (netype != null)
                        tok.props.put("ne", netype);
                    
                    sentdiv.add(tok);
                }
                paradiv.add(sentdiv);
            }
            doc.add(paradiv);
        }
    }
}

