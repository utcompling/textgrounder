///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.models;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIterator;
import gnu.trove.TIntObjectHashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.KMLUtil;

/**
 * Base abstract class for all training models. Defines some methods for
 * interfacing with gazetteers. Declares some fields that deal with regions
 * and placenames.
 * 
 * @author 
 */
public abstract class Model {

    // Minimum number of pixels the (small) square region (NOT our Region) represented by each city must occupy on the screen for its label to appear:
    public final static int MIN_LOD_PIXELS = 16;
    /**
     * Number of paragraphs to consider as one document.
     */
    protected int paragraphsAsDocs;
    /**
     * Dimensions of regionArray.
     */
    protected int regionArrayWidth, regionArrayHeight;
    /**
     * Array of locations that have been identified in training data
     */
    protected TIntHashSet locations;
    /**
     * Path to training data. May be directory or file
     */
    protected String trainInputPath;
    /**
     * File instantiation of trainInputPath
     */
    protected File trainInputFile;
    /**
     * Name of kml file (i.e. Google Earth format xml) to generate output to
     */
    protected String kmlOutputFilename;
    /**
     * Height of bars for Google Earth KML output
     */
    protected int barScale = 50000;
    /**
     * Gazetteer that holds geographic information
     */
    protected Gazetteer gazetteer;
    /**
     * Array of array of toponym indices by document. This includes only the
     * toponyms and none of the non-toponyms.
     */
    protected TextProcessor textProcessor;
    /**
     * Lookup table for Location (hash)code to Location object
     */
    protected TIntObjectHashMap<Location> idxToLocationMap;
    /**
     * Size of regions on cartesian reduction of globe. Globe is defined to be
     * 360 degrees longitude and 180 degrees latitude
     */
    protected double degreesPerRegion;
    /**
     * Array of regions defined by dividing globe dimensions by degreesPerRegion.
     */
    protected Region[][] regionArray;
    /**
     * Number of regions which have been activated in regionArray
     */
    protected int activeRegions;
    /**
     * Collection of training data
     */
    protected Lexicon lexicon;
    /**
     * Model iterations (e.g. for ProbabilisticMinDistanceModel)
     */
    protected int modelIterations;
    /**
     * Evaluation directory; null if no evaluation is to be done.
     */
    protected String evalInputPath = null;
    /**
     * File instantiation of evalInputPath
     */
    protected File evalInputFile;
    /**
     * Flag that tells system to ignore the input file(s) and instead run on every locality in the gazetteer
     */
    protected boolean runWholeGazetteer = false;
    /**
     * Number of words to print on both sides of toponym in kml context output
     */
    protected int windowSize;
    /**
     * Array of token indices and associated information for training data
     */
    protected TokenArrayBuffer trainTokenArrayBuffer;
    /**
     * Array of token indices and associated information for eval data
     */
    protected TokenArrayBuffer evalTokenArrayBuffer;
    //protected int indexInTAB = 0;

    public Model() {
    }

    public Model(Gazetteer gaz, int bscale, int paragraphsAsDocs) {
        barScale = bscale;
        gazetteer = gaz;
        lexicon = new Lexicon();
    }

    public Model(CommandLineOptions options) {
        try {
            initialize(options);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param options
     */
    protected void initialize(CommandLineOptions options) throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {
        runWholeGazetteer = options.getRunWholeGazetteer();
        evalInputPath = options.getEvalDir();

        if (options.getTrainInputPath() != null) {
            trainInputPath = options.getTrainInputPath();
            if (trainInputPath == null) {
                System.out.println("Error: You must specify an input filename with the -i flag.");
                System.exit(0);
            }
            trainInputFile = new File(trainInputPath);
        }

        if (runWholeGazetteer) {
            trainInputPath = null;
            trainInputFile = null;
        } else if (evalInputPath != null) {
            //System.out.println(evalInputPath);
            //System.exit(0);
            evalInputFile = new File(evalInputPath);
            assert (evalInputFile.isDirectory());
            //System.exit(0);
        }

        String gazTypeArg = options.getGazetteType().toLowerCase();
        if (gazTypeArg.startsWith("c")) {
            gazetteer = new CensusGazetteer(options);
        } else if (gazTypeArg.startsWith("n")) {
            gazetteer = new NGAGazetteer(options);
        } else if (gazTypeArg.startsWith("u")) {
            gazetteer = new USGSGazetteer(options);
        } else if (gazTypeArg.startsWith("w")) {
            gazetteer = new WGGazetteer(options);
        } else if (gazTypeArg.startsWith("t")) {
            gazetteer = new TRGazetteer(options);
        } else {
            System.err.println("Error: unrecognized gazetteer type: " + gazTypeArg);
            System.err.println("Please enter w, c, u, g, or t.");
            System.exit(0);
            //myGaz = new WGGazetteer();
        }

        modelIterations = options.getModelIterations();

        kmlOutputFilename = options.getKMLOutputFilename();
        degreesPerRegion = options.getDegreesPerRegion();

        if (!runWholeGazetteer) {
            paragraphsAsDocs = options.getParagraphsAsDocs();
            lexicon = new Lexicon();

            String fname = trainInputFile.getName();
            if (options.isPCLXML()) {
                textProcessor = new TextProcessorPCLXML(lexicon);
            } else if (trainInputFile.isDirectory() && trainInputFile.list(new PCLXMLFilter()).length != 0) {
                textProcessor = new TextProcessorPCLXML(lexicon);
            } else if (fname.startsWith("txu") && fname.endsWith(".xml")) {
                textProcessor = new TextProcessorPCLXML(lexicon);
            } else if (evalInputPath == null) {
                textProcessor = new TextProcessor(lexicon, paragraphsAsDocs);
            } else {
                textProcessor = new TextProcessor(lexicon, paragraphsAsDocs, true);
            }
        }

        barScale = options.getBarScale();

        windowSize = options.getWindowSize();
    }

    public void initializeRegionArray() {
        activeRegions = 0;

        regionArrayWidth = 360 / (int) degreesPerRegion;
        regionArrayHeight = 180 / (int) degreesPerRegion;

        regionArray = new Region[regionArrayWidth][regionArrayHeight];
        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                regionArray[w][h] = null;
            }
        }
    }

    public void printRegionArray() {
        System.out.println();

        for (int h = 0; h < regionArrayHeight; h++) {
            for (int w = 0; w < regionArrayWidth; w++) {
                if (regionArray[w][h] == null) {
                    System.out.print("0");
                } else {
                    System.out.print("1");
                }
            }
            System.out.println();
        }

        System.out.println(activeRegions + " active regions for this document out of a possible "
              + (regionArrayHeight * regionArrayWidth) + " (region size = "
              + degreesPerRegion + " x " + degreesPerRegion + " degrees).");
    }

    public void activateRegionsForWholeGaz() throws Exception {
        System.out.println("Running whole gazetteer through system...");

        //locations = new ArrayList<Location>();
        locations = gazetteer.getAllLocalities();
        addLocationsToRegionArray(locations);
        //locations = null; //////////////// uncomment this to get a null pointer but much faster termination
        //                                   if you only want to know the number of active regions :)
    }

    /**
     * Remove punctuation from first and last characters of a string
     *
     * @param aString String to strip
     * @return Input stripped of punctuation
     */
    protected String stripPunc(String aString) {
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(0))) {
            aString = aString.substring(1);
        }
        while (aString.length() > 0 && !Character.isLetterOrDigit(aString.charAt(aString.length() - 1))) {
            aString = aString.substring(0, aString.length() - 1);
        }
        return aString;
    }

    /**
     * Add locations to the 2D regionArray. To be used by classes that
     * do not use a RegionMapperCallback.
     *
     * @param locs list of locations.
     */
    protected void addLocationsToRegionArray(TIntHashSet locs) {
        addLocationsToRegionArray(locs, gazetteer, new NullRegionMapperCallback());
    }

    /**
     * Add locations to 2D regionArray. To be used by classes and methods that
     * use a RegionMapperCallback
     *
     * @param locs list of locations
     * @param regionMapper callback class for handling mappings of locations
     * and regions
     */
    protected void addLocationsToRegionArray(TIntHashSet locs, Gazetteer gaz,
          RegionMapperCallback regionMapper) {
        for (TIntIterator it = locs.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = gaz.getLocation(locid);
            /*if(loc.coord.latitude < -180.0 || loc.coord.longitude > 180.0
            || loc.coord.longitude < -90.0 || loc.coord.longitude > 900) {
            // switched?
            }*/
            int curX = (int) (loc.coord.latitude + 180) / (int) degreesPerRegion;
            int curY = (int) (loc.coord.longitude + 90) / (int) degreesPerRegion;
            //System.out.println(loc.coord.latitude + ", " + loc.coord.longitude + " goes to");
            //System.out.println(curX + " " + curY);
            if (curX < 0 || curY < 0) {
                if (curX < 0) {
                    curX = 0;
                }
                if (curY < 0) {
                    curY = 0;
                }
                System.err.println("Warning: " + loc.name + " had invalid coordinates (" + loc.coord + "); mapping it to [" + curX + "][" + curY + "]");
            }
            if (regionArray[curX][curY] == null) {
                double minLon = loc.coord.longitude - loc.coord.longitude % degreesPerRegion;
                double maxLon = minLon + (loc.coord.longitude < 0 ? -1 : 1) * degreesPerRegion;
                double minLat = loc.coord.latitude - loc.coord.latitude % degreesPerRegion;
                double maxLat = minLat + (loc.coord.latitude < 0 ? -1 : 1) * degreesPerRegion;
                regionArray[curX][curY] = new Region(minLon, maxLon, minLat, maxLat);
                activeRegions++;
            }
            regionMapper.addToPlace(loc, regionArray[curX][curY]);
        }
    }

    /**
     * Process training data. Populate lexicon, identify toponyms, build arrays.
     *
     * @throws Exception
     */
    public void processTrainInputPath() throws Exception {
        trainTokenArrayBuffer = new TokenArrayBuffer(lexicon);
        processTrainInputPath(trainInputFile, textProcessor, trainTokenArrayBuffer, new NullStopwordList());
        trainTokenArrayBuffer.convertToPrimitiveArrays();
    }

    /**
     * 
     * @param myPath
     * @param textProcessor
     * @param trainTokenArrayBuffer
     * @param stopwordList
     * @throws IOException
     */
    public void processTrainInputPath(File myPath, TextProcessor textProcessor,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws
          IOException {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processTrainInputPath(new File(myPath.getCanonicalPath() + File.separator + pathname), textProcessor, tokenArrayBuffer, stopwordList);
            }
        } else {
            textProcessor.addToponymsFromFile(myPath.getCanonicalPath(), tokenArrayBuffer, stopwordList);
        }
    }

    /**
     * Process training data. Populate lexicon, identify toponyms, build arrays.
     *
     * @throws Exception
     */
    public void processEvalInputPath() throws Exception {
        evalTokenArrayBuffer = new TokenArrayBuffer(lexicon);
        processTrainInputPath(evalInputFile, textProcessor, evalTokenArrayBuffer, new NullStopwordList());
        evalTokenArrayBuffer.convertToPrimitiveArrays();
    }

    /**
     *
     * @param myPath
     * @param textProcessor
     * @param trainTokenArrayBuffer
     * @param stopwordList
     * @throws IOException
     */
    public void processEvalInputPath(File myPath, TextProcessor textProcessor,
          TokenArrayBuffer tokenArrayBuffer, StopwordList stopwordList) throws
          IOException {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processTrainInputPath(new File(myPath.getCanonicalPath() + File.separator + pathname), textProcessor, tokenArrayBuffer, stopwordList);
            }
        } else {
            textProcessor.addToponymsFromGoldFile(myPath.getCanonicalPath(), tokenArrayBuffer, stopwordList);
        }
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     * 
     * @throws Exception
     */
    public void writeXMLFile() throws Exception {
        if (!runWholeGazetteer) {
            writeXMLFile(trainInputPath, kmlOutputFilename, locations);
        } else {
            writeXMLFile("WHOLE_GAZETTEER", kmlOutputFilename, locations);
        }
    }

    /**
     * Wrapper for writeXMLFile when gazetteer is known
     *
     * @param inputFilename
     * @param outputFilename
     * @param locations
     * @throws IOException
     */
    public void writeXMLFile(String inputFilename, String outputFilename,
          TIntHashSet locations) throws IOException {
        writeXMLFile(inputFilename, outputFilename, gazetteer, locations);
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     *
     * @param trainInputPath
     * @param kmlOutputFilename
     * @param gaz 
     * @param locations
     * @throws Exception
     */
    public void writeXMLFile(String inputFilename, String outputFilename,
          Gazetteer gaz, TIntHashSet locations) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
        int dotKmlIndex = outputFilename.lastIndexOf(".kml");
        String contextFilename = outputFilename.substring(0, dotKmlIndex) + "-context.kml";
        BufferedWriter contextOut = new BufferedWriter(new FileWriter(contextFilename));

        out.write(KMLUtil.genKMLHeader(inputFilename));

        contextOut.write(KMLUtil.genKMLHeader(inputFilename));

        /*TObjectIntIterator<String> placeIterator = placeCounts.iterator();
        for (int i = placeCounts.size(); i-- > 0;) {
        placeIterator.advance();
        String placename = placeIterator.key();
        double height = Math.log(placeIterator.value()) * barScale;

        Coordinate coord;
        if(gazetteer instanceof WGGazetteer)
        coord = ((WGGazetteer)gazetteer).baselineGet(placename);
        else
        coord = gazetteer.get(placename);

        if(coord.longitude == 9999.99) // sentinel
        continue;*/
        for (TIntIterator it = locations.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = gaz.getIdxToLocationMap().get(locid);

            double height = Math.log(loc.count) * barScale;

            double radius = .15;
            //String kmlPolygon = coord.toKMLPolygon(4,radius,height);  // a square
            String kmlPolygon = loc.coord.toKMLPolygon(10, radius, height);

            String placename = loc.name;
            Coordinate coord = loc.coord;
            out.write(KMLUtil.genPolygon(placename, coord, radius, kmlPolygon));

            //System.out.println("Contexts for " + placename);
	    /*while(indexInTAB < trainTokenArrayBuffer.toponymVector.size() && trainTokenArrayBuffer.toponymVector.get(indexInTAB) == 0) {
            indexInTAB++;
            }*/
            for (int j = 0; j < loc.backPointers.size(); j++) {
                int index = loc.backPointers.get(j);
                String context = trainTokenArrayBuffer.getContextAround(index, windowSize, true);
                Coordinate spiralPoint = coord.getNthSpiralPoint(j, 0.13);

                contextOut.write(KMLUtil.genSpiralpoint(placename, context, spiralPoint, j, radius));
            }
            //indexInTAB++;

            //if(i >= 10) System.exit(0);

        }

        out.write(KMLUtil.genKMLFooter());
        contextOut.write(KMLUtil.genKMLFooter());

        out.close();
        contextOut.close();
    }

    public void evaluate() {

        if (trainTokenArrayBuffer.modelLocationArrayList.size() != trainTokenArrayBuffer.goldLocationArrayList.size()) {
            System.out.println("MISMATCH: model: " + trainTokenArrayBuffer.modelLocationArrayList.size() + "; gold: " + trainTokenArrayBuffer.goldLocationArrayList.size());
            System.exit(0);
        }

        //int numTrue = 0;
        int tp = 0;
        int fp = 0;
        int fn = 0;

        for (int i = 0; i < trainTokenArrayBuffer.size(); i++) {
            Location curModelLoc = trainTokenArrayBuffer.modelLocationArrayList.get(i);
            Location curGoldLoc = trainTokenArrayBuffer.goldLocationArrayList.get(i);
            if (curGoldLoc != null) {
                if (curModelLoc != null) {
                    if (curGoldLoc.looselyMatches(curModelLoc, 1.0)) {
                        tp++;
                    } else {
                        fp++;
                        //fn++; // just a FP according to Jason
                    }
                } else {
                    fn++;
                }
            } else {
                if (curModelLoc != null) {
                    fp++;
                } else {
                    //tn++;
                }
            }
        }

        double precision = (double) tp / (tp + fp);
        double recall = (double) tp / (tp + fn);
        double f1 = 2 * ((precision * recall) / (precision + recall));

        System.out.println("TP: " + tp);
        System.out.println("FP: " + fp);
        System.out.println("FN: " + fn);
        System.out.println();
        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F-score: " + f1);
    }

    /**
     * Train model. For access from main routines.
     */
    public abstract void train();

    /**
     * @return the kmlOutputFilename
     */
    public String getOutputFilename() {
        return kmlOutputFilename;
    }

    /**
     * @return the trainInputFile
     */
    public File getInputFile() {
        return trainInputFile;
    }

    /**
     * @return the textProcessor
     */
    public TextProcessor getTextProcessor() {
        return textProcessor;
    }

    class PCLXMLFilter implements FilenameFilter {

        public boolean accept(File dir, String name) {
            return (name.startsWith("txu") && name.endsWith(".xml"));
        }
    }
}
