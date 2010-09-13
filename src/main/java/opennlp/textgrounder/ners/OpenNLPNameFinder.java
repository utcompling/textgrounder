///////////////////////////////////////////////////////////////////////////////
//Copyright (C) 2003 Thomas Morton
//Copyright (C) 2010 Ben Wing
// 
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
// 
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//GNU Lesser General Public License for more details.
// 
//You should have received a copy of the GNU Lesser General Public
//License along with this program; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//////////////////////////////////////////////////////////////////////////////

// package opennlp.tools.lang.english;
package opennlp.textgrounder.ners;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import opennlp.maxent.MaxentModel;
import opennlp.maxent.io.PooledGISModelReader;
import opennlp.tools.namefind.NameFinderEventStream;
import opennlp.tools.namefind.NameFinderME;
// import opennlp.tools.parser.Parse;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.Span;

/**
 * This class is used to create a name finder (i.e. named-entity recognizer, aka
 * NER) for English. The code in this class is based on the code in
 * NameFinder.java in package opennlp.tools.lang.english. For various reasons it
 * was necessary to copy this code and rewrite it rather than trying to
 * subclass:
 * <ol>
 * <li>The non-public methods are declared private (oops) and can't be called by
 * a subclass.
 * <li>There's no interface to access the separate tokens that are determined as
 * part of the NER process.
 * <li>More generally there's no good interface.
 * </ol>
 * 
 * @author Thomas Morton (original author of code in opennlp.tools.lang.english)
 * @author Ben Wing (adapted for use in TextGrounder) 
 * 
 */
public class OpenNLPNameFinder {

    public static String[] NAME_TYPES = { "person", "organization", "location",
            "date", "time", "percentage", "money" };

    protected NameFinderME nameFinder;

    /**
     * Creates an English name finder using the specified model.
     * 
     * @param mod
     *            The model used for finding names.
     */
    public OpenNLPNameFinder(MaxentModel mod) {
        nameFinder = new NameFinderME(mod);
    }

    protected static void clearPrevTokenMaps(OpenNLPNameFinder[] finders) {
        for (int mi = 0; mi < finders.length; mi++) {
            finders[mi].nameFinder.clearAdaptiveData();
        }
    }

    /**
     * Process the given input through a named-entity recognizer. Should have
     * one sentence per line, with blank lines used to separate paragraphs.
     * Returns a list of paragraphs, each of which is a list of sentences, each
     * of which is a list of tokens, each of which is a two-element array of
     * (token-string, token-type). The token type is either a string indicating
     * the type of named entity, or null for non-entity tokens. The possible
     * types are listed above in NAME_TYPES.
     * 
     * @param finders
     *            The name finders to be used.
     * @param tags
     *            The tag names for the corresponding name finder.
     * @param input
     *            The input reader.
     * @throws IOException
     */
    protected static List<List<List<String[]>>> processText(
            OpenNLPNameFinder[] finders, String[] tags, BufferedReader input)
            throws IOException {
        Span[][] nameSpans = new Span[finders.length][];
        String[][] nameOutcomes = new String[finders.length][];
        opennlp.tools.tokenize.Tokenizer tokenizer = new SimpleTokenizer();
        List<List<List<String[]>>> paras = new ArrayList<List<List<String[]>>>();
        List<List<String[]>> curpara = new ArrayList<List<String[]>>();
        for (String line = input.readLine(); null != line; line = input
                .readLine()) {
            List<String[]> cursent = new ArrayList<String[]>();
            if (line.equals("")) {
                clearPrevTokenMaps(finders);
                if (!curpara.isEmpty())
                    paras.add(curpara);
                curpara = new ArrayList<List<String[]>>();
                continue;
            }
            Span[] spans = tokenizer.tokenizePos(line);
            String[] tokens = Span.spansToStrings(spans, line);
            // System.out.println("Tokens on line (not including braces):");
            // for (int i = 0; i < tokens.length; i++) {
            // System.out.println("Token #" + i + ": {" + tokens[i] + "}");
            // }
            for (int fi = 0, fl = finders.length; fi < fl; fi++) {
                nameSpans[fi] = finders[fi].nameFinder.find(tokens);
                // System.err.println("EnglighOpenNLPNameFinder.processText: "+tags[fi]
                // + " " + java.util.Arrays.asList(finderTags[fi]));
                nameOutcomes[fi] = NameFinderEventStream.generateOutcomes(
                        nameSpans[fi], null, tokens.length);
            }

            String token = "";
            boolean inToken = false;
            for (int ti = 0, tl = tokens.length; ti < tl; ti++) {
                for (int fi = 0, fl = finders.length; fi < fl; fi++) {
                    // check for end tags
                    if (ti != 0) {
                        if ((nameOutcomes[fi][ti].equals(NameFinderME.START) || nameOutcomes[fi][ti]
                                .equals(NameFinderME.OTHER))
                                && (nameOutcomes[fi][ti - 1]
                                        .equals(NameFinderME.START) || nameOutcomes[fi][ti - 1]
                                        .equals(NameFinderME.CONTINUE))) {
                            // Processing end of token of type tags[fi]
                            cursent.add(new String[] { token, tags[fi] });
                            // System.out.println("Saw token {" + token
                            // + "} of type " + tags[fi]);
                            inToken = false;
                        }
                    }
                }
                // add separation (non-token) text to token
                if (ti > 0 && spans[ti - 1].getEnd() < spans[ti].getStart()) {
                    token += line.substring(spans[ti - 1].getEnd(),
                            spans[ti].getStart());
                }
                // check for start tags
                for (int fi = 0, fl = finders.length; fi < fl; fi++) {
                    if (nameOutcomes[fi][ti].equals(NameFinderME.START)) {
                        // Processing start of token of type tags[fi]
                        // output.append("<").append(tags[fi]).append(">");
                        token = "";
                        inToken = true;
                    }
                }
                // add current-token text to token
                token += tokens[ti];
                if (!inToken) {
                    cursent.add(new String[] { tokens[ti], null });
                    // System.out.println("Saw token {" + tokens[ti]
                    // + "} of no type");
                }
            }
            // final end tags
            if (tokens.length != 0) {
                for (int fi = 0, fl = finders.length; fi < fl; fi++) {
                    if (nameOutcomes[fi][tokens.length - 1]
                            .equals(NameFinderME.START)
                            || nameOutcomes[fi][tokens.length - 1]
                                    .equals(NameFinderME.CONTINUE)) {
                        // Processing end of token of type tags[fi]
                        cursent.add(new String[] { token, tags[fi] });
                        // System.out.println("Saw token {" + token +
                        // "} of type " + tags[fi]);
                    }
                }
            }
            // Handle final non-token text
            if (tokens.length != 0) {
                if (spans[tokens.length - 1].getEnd() < line.length()) {
                    // System.out.println("Remaining non-token text {"
                    // + line.substring(spans[tokens.length - 1].getEnd())
                    // + "} at eol");
                }
            }
            curpara.add(cursent);
        }
        if (!curpara.isEmpty())
            paras.add(curpara);
        return paras;
    }

    /**
     * Do named-entity recognition on the given text using the given models.
     * Should have one sentence per line, with blank lines used to separate
     * paragraphs. Returns a list of paragraphs, each of which is a list of
     * sentences, each of which is a list of tokens, each of which is a
     * two-element array of (token-string, token-type). The token type is either
     * a string indicating the type of named entity, or null for non-entity
     * tokens. The possible types are listed above in NAME_TYPES.
     * 
     * @param models
     * @param text
     * @return
     * @throws IOException
     */
    public static List<List<List<String[]>>> run(List<String> models,
            String text) throws IOException {
        OpenNLPNameFinder[] finders = new OpenNLPNameFinder[models.size()];
        String[] names = new String[models.size()];
        for (int i = 0; i < models.size(); i++) {
            String modelName = models.get(i);
            finders[i] = new OpenNLPNameFinder(new PooledGISModelReader(
                    new File(modelName)).getModel());
            int nameStart = modelName.lastIndexOf(System
                    .getProperty("file.separator")) + 1;
            int nameEnd = modelName.indexOf('.', nameStart);
            if (nameEnd == -1) {
                nameEnd = modelName.length();
            }
            names[i] = modelName.substring(nameStart, nameEnd);
        }
        BufferedReader in = new BufferedReader(new StringReader(text));
        return processText(finders, names, in);
    }
}

