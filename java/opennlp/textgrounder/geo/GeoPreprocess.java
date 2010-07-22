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

import org.apache.commons.cli.*;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.ners.*;

/**
 * Application for preprocessing the input and converting it to a standard XML format.
 * 
 * @author benwing
 */
public class GeoPreprocess {
    /**
     * Type of gazette. The gazette shorthands from the commandline are:
     * <pre>
     * "c": CensusGazetteer
     * "n": NGAGazetteer
     * "u": USGSGazetteer
     * "w": WGGazetteer (default)
     * "t": TRGazetteer
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
     * Path to input data. Can be directory or a single file.
     */
    protected String inputPath = null;
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
     * Whether format of input data is in tei encoded pcl-travel xml file
     */
    protected boolean PCLXML = false;

    /**
     * NamedEntityRecognizer object.
     * 
     * FIXME: Should be specifiable by command-line option.
     */
    protected NamedEntityRecognizer ner = new StanfordNER();

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
            case 'g':
                opt = option.getOpt();
                if (opt.equals("g")) {
                    gazetteType = value;
                } else if (opt.equals("gr")) {
                    gazetteerRefresh = true;
                }
                break;
            case 'i':
                opt = option.getOpt();
                if (opt.equals("i")) {
                    inputPath = value;
                } else if (opt.equals("id")) {
                    gazetteerPath = canonicalPath(value);
                }
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
            case 'x':
                PCLXML = true;
                break;
            }
        }
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
        try {
            if (filename.endsWith(".xml"))
                TextProcessor.processXML(filename, corpus);
            else if (filename.endsWith(".tr"))
                TextProcessor.processTR(filename, corpus);
            else if (!filename.endsWith(".DS_Store") && !filename.endsWith(".dtd"))
                TextProcessor.processNER(filename, corpus, ner,
                        paragraphsAsDocs);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * Takes an Options instance and adds/sets command line options.
     *
     * @param options Command line options
     */
    protected static void setOptions(Options options) {
        options.addOption("ev", "evaluate", true, "specifies evaluation directory and enables evaluation mode");
        options.addOption("g", "gazetteer", true, "gazetteer to use [world, census, NGA, USGS; default = world]");
        options.addOption("gr", "gazetteer-refresh", false, "read gazetteer afresh from original database");
        options.addOption("i", "nput", true, "input file or directory name");
        options.addOption("id", "gazetteer-path", true, "path to gazetteer db");
        options.addOption("o", "output", true, "output filename");
        options.addOption("p", "paragraphs-as-docs", true, "number of paragraphs to treat as a document. Set the argument to 0 if documents should be treated whole");
        options.addOption("wg", "whole-gazetteer", false, "activate regions and run program for entire gazetteer (the -i flag will be ignored in this case)");
        options.addOption("x", "pclxml", false, "whether format of input file is pcl-travel xml format or not");

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
                for (String pathname : file.list()) {
                    System.out.println("Processing: " + pathname);
                    gp.processFile(file.getCanonicalPath() + File.separator + pathname,
                            corpus);
                }
            } else
                gp.processFile(filename, corpus);
        }

        System.out.println("Writing output file " + gp.outputPath);
        corpus.outputXML(new File(gp.outputPath));
    }
        
}
