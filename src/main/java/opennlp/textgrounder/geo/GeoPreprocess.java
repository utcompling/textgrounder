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
package opennlp.textgrounder.geo;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.cli.*;

import opennlp.textgrounder.gazetteers.old.Gazetteer;
import opennlp.textgrounder.gazetteers.old.GazetteerGenerator;
import opennlp.textgrounder.textstructs.Corpus;
import opennlp.textgrounder.textstructs.TextProcessor;
import opennlp.textgrounder.textstructs.old.Lexicon;
import opennlp.textgrounder.ners.*;

/**
 * Application for preprocessing the input and converting it to a standard XML format.
 * 
 * @author benwing
 */
public class GeoPreprocess {
    /**
     * Type of gazette. The gazette shorthands from the command line are:
     * <pre>
     * "c": CensusGazetteer
     * "n": NGAGazetteer
     * "u": USGSGazetteer
     * "w": WGGazetteer (default)
     * "t": TRGazetteer
     * "g": NGGazetteer
     * </pre>
     */
    protected String gazetteType = "w";
    /**
     * Flag for refreshing gazetteer from original database
     */
    protected boolean gazetteerRefresh = false;
    /**
     * Path to gazetteer database
     */
    protected String gazetteerPath = "/tmp/toponym.db";
    /**
     * Path of output data.
     */
    protected String outputPath = null;
    /**
     * Number of paragraphs to treat as a single document.
     */
    protected int paragraphsAsDocs = 10;
    /**
     * A flag that runs the system on every locality in the gazetteer, ignoring the input text file(s), if set to true
     */
    protected boolean runWholeGazetteer = false;
    /**
     * Format of input. The possible formats are:
     * <pre>
     * "auto": Figure out the format based on the file's extension and possibly its contents
     * "raw": Raw text (will be passed through NER)
     * "teixml": TEI-encoded PCL-travel XML format (will be passed through NER)
     * "tr": TR-CONLL format
     * "trxml": TR-CONLL XML format (also used for output)
     * </pre>
     */
    protected String format = "auto";
    /**
     * Named entity recognizer (NER) used to process raw text, etc. The possible types are:
     * <pre>
     * "stanford": The Stanford NER
     * "opennlp": The NER that comes as part of OpenNLP
     * </pre>
     */
    protected String nerType = "stanford";

    /**
     * NamedEntityRecognizer object.
     * 
     * FIXME: Should be specifiable by command-line option.
     */
    protected NamedEntityRecognizer ner;

    /**
     * 
     * @param cline
     * @throws IOException
     */
    public GeoPreprocess(CommandLine cline) throws IOException {

        String opt = null;

        for (Option option : cline.getOptions()) {
            String value = option.getValue();
            switch (option.getOpt().charAt(0)) {
            case 'f':
                format = value;
                break;
            case 'g':
                opt = option.getOpt();
                if (opt.equals("g")) {
                    gazetteType = value;
                } else if (opt.equals("gr")) {
                    gazetteerRefresh = true;
                }
                break;
            case 'i':
                gazetteerPath = canonicalPath(value);
                break;
            case 'n':
                nerType = value;
                break;
            case 'o':
                outputPath = value;
                break;
            case 'p':
                paragraphsAsDocs = Integer.parseInt(value);
                break;
            case 'w':
                if (option.getOpt().equals("wg")) {
                    runWholeGazetteer = true;
                }
                break;
            }
        }
        
        if (nerType.equals("stanford"))
            ner = new StanfordNER();
        else
            ner = new OpenNLPNER();
    }

    protected String canonicalPath(String _path) {
        try {
            String home = System.getProperty("user.home");
            String path = "";
            path = _path.replaceAll("~", home);
            path = (new File(path)).getCanonicalPath();
            return path;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected void processFile(String filename, Corpus corpus) {
        if (format.equals("auto")) {
            if (filename.endsWith(".xml"))
                TextProcessor.processXML(filename, corpus);
            else if (filename.endsWith(".tr"))
                TextProcessor.processTR(filename, corpus);
            else if (!filename.endsWith(".DS_Store")
                    && !filename.endsWith(".dtd"))
                TextProcessor.processNER(filename, corpus, ner,
                        paragraphsAsDocs);
        } else if (format.equals("raw"))
            TextProcessor.processNER(filename, corpus, ner, paragraphsAsDocs);
        else if (format.equals("teixml"))
            TextProcessor.processTEIXML(filename, corpus, ner);
        else if (format.equals("tr"))
            TextProcessor.processTR(filename, corpus);
        else if (format.equals("trxml"))
            TextProcessor.processXML(filename, corpus);
    }

    /**
     * Takes an Options instance and adds/sets command line options.
     *
     * @param options Command line options
     */
    protected static void setOptions(Options options) {
        options.addOption("g", "gazetteer", true, "gazetteer to use [world, census, NGA, USGS, GeoNames; default = world]");
        options.addOption("gr", "gazetteer-refresh", false, "read gazetteer afresh from original database");
        options.addOption("id", "gazetteer-path", true, "path to gazetteer db");
        options.addOption("o", "output", true, "output filename");
        options.addOption("p", "paragraphs-as-docs", true, "number of paragraphs to treat as a document. Set the argument to 0 if documents should be treated whole");
        options.addOption("wg", "whole-gazetteer", false, "activate regions and run program for entire gazetteer (the -i flag will be ignored in this case)");
        options.addOption("f", "format", true, "format of input [auto, tr, teixml, raw, trxml, default = auto]");
        options.addOption("n", "ner", true, "named entity recognizer to use [stanford, opennlp, default = stanford]");

        options.addOption("h", "help", false, "print help");
    }
    
    public static void main(String[] args) throws Exception {

        CommandLineParser optparse = new PosixParser();

        Options options = new Options();
        setOptions(options);

        CommandLine cline = optparse.parse(options, args);

        if (cline.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java GeoPreprocess", options);
            System.exit(0);
        }

        GeoPreprocess gp = new GeoPreprocess(cline);
        Lexicon lexicon = new Lexicon();
        Gazetteer gazetteer = new GazetteerGenerator(gp.gazetteType,
                gp.gazetteerPath, gp.gazetteerRefresh).generateGazetteer();
        Corpus corpus = new Corpus(gazetteer, lexicon);
        
        for (String filename : cline.getArgs()) {
            System.out.println("Processing: " + filename);
            File file = new File(filename);
            if (file.isDirectory()) {
                File[] contents = file.listFiles();
                Arrays.sort(contents);
                for (File dirFile : contents) {
                    System.out.format("Processing: %s\n", dirFile);
                    gp.processFile(dirFile.getPath(), corpus);
                }
            } else
                gp.processFile(filename, corpus);
        }

        System.out.println("Writing output file " + gp.outputPath);
        corpus.outputXML(new File(gp.outputPath));
    }
        
}

