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

import java.io.*;
import java.util.zip.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.KMLUtil;

/**
 * Base abstract class for all models. A "model" is a class that defines methods
 * that determine how to map a toponym to a location. A toponym is a string
 * naming a location, potentially ambiguous in that many locations may have the
 * same name. Some models use a built-in heuristic (e.g. pick the location with
 * the highest population), and some learn the mapping using training data. The
 * former type of model has special methods that are encapsulated in the
 * SelfTrainedModelBase subclass.
 *
 * Defines some methods for interfacing with gazetteers. Declares some fields
 * that deal with regions and placenames.
 *
 * @author
 */
public abstract class Model {

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
    protected String trainInputPath = null;
    /**
     * File instantiation of trainInputPath
     */
    protected File trainInputFile = null;
    /**
     * Name of kml file (i.e. Google Earth format xml) to generate output to
     */
    protected String kmlOutputFilename;
    /**
     * Height of bars for Google Earth KML output
     */
    protected int barScale = 50000;
    /**
     * Size of regions on cartesian reduction of globe. Globe is defined to be
     * 360 degrees longitude and 180 degrees latitude
     */
    protected double degreesPerRegion;
    /**
     * Two-dimensional array of Region objects, one for each 3x3-degree (or
     * whatever, based on `degreesPerRegion') region of the earth. The
     * dimensions are determined by dividing the globe dimensions (360 degrees
     * of latitude, 180 degrees of longitude) by `degreesPerRegion'.
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
     * Flag that tells system to ignore the input file(s) and instead run on
     * every locality in the gazetteer
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
    protected EvalTokenArrayBuffer evalTokenArrayBuffer;
    /**
     * Generates gazetteers as local variables. Global if necessary. Reduces
     * memory consumption
     */
    protected GazetteerGenerator gazetteerGenerator;
    /**
     * Flag for refreshing gazetteer from original database
     */
    protected boolean gazetteerRefresh = false;

    public Model() {
    }

    /**
     * Normal constructor. Initialize member variables based on command-line
     * options. See initialize().
     * 
     */
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
     * Initialize member variables based on command-line options. Creates a
     * Lexicon and a gazetteer generator but doesn't generate the gazetteer.
     * (That happens e.g. in the constructor for SelfTrainedModelBase.)
     * 
     * @param options
     */
    protected void initialize(CommandLineOptions options)
            throws FileNotFoundException, IOException, ClassNotFoundException,
            SQLException {
        runWholeGazetteer = options.getRunWholeGazetteer();
        evalInputPath = options.getEvalDir();

        gazetteerRefresh = options.getGazetteerRefresh();

        /*
         * Set up trainInputPath from options, create a File object in
         * trainInputFile
         */
        if (options.getTrainInputPath() != null) {
            trainInputPath = options.getTrainInputPath();
            if (trainInputPath == null) {
                System.out
                        .println("Error: You must specify an input filename with the -i flag.");
                System.exit(0);
            }
            trainInputFile = new File(trainInputPath);
        }

        if (runWholeGazetteer) {
            trainInputPath = null;
            trainInputFile = null;
        }

        /* Same thing for the eval directory, if it exists */
        if (evalInputPath != null) {
            // System.out.println(evalInputPath);
            // System.exit(0);
            evalInputFile = new File(evalInputPath);
            assert (evalInputFile.isDirectory());
            // System.exit(0);
        }

        modelIterations = options.getModelIterations();
        kmlOutputFilename = options.getKMLOutputFilename();
        degreesPerRegion = options.getDegreesPerRegion();
        paragraphsAsDocs = options.getParagraphsAsDocs();
        barScale = options.getBarScale();
        windowSize = options.getWindowSize();

        lexicon = new Lexicon();
        gazetteerGenerator = new GazetteerGenerator(options.getGazetteType(),
                options.getGazetteerPath(), options.getGazetteerRefresh());
    }

    /**
     * Initialize the region array to be of the right size and contain null
     * pointers. (FIXME: How do the Region objects get instantiated?)
     */
    public void initializeRegionArray() {
        activeRegions = 0;

        regionArrayWidth = 360 / (int) degreesPerRegion;
        regionArrayHeight = 180 / (int) degreesPerRegion;

        regionArray = new Region[regionArrayWidth][regionArrayHeight];
        /*
         * FIXME: Why are we setting the values to null? Surely they must
         * already start out this way?
         */
        for (int w = 0; w < regionArrayWidth; w++) {
            for (int h = 0; h < regionArrayHeight; h++) {
                regionArray[w][h] = null;
            }
        }
    }

    /*
     * Print out the region array as a 2D ASCII array of 0's and 1's, indicating
     * whether each region is "active" (corresponding Region object exists).
     */
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

        System.out.println(activeRegions
                + " active regions for this document out of a possible "
                + (regionArrayHeight * regionArrayWidth) + " (region size = "
                + degreesPerRegion + " x " + degreesPerRegion + " degrees).");
    }

    /**
     * Add locations to 2D regionArray. To be used by classes and methods that
     * use a RegionMapperCallback.
     * 
     * @see addLocationsToRegionArray(E loc, RegionMapperCallback regionMapper)
     * 
     * @param locs
     *            list of locations
     * @param regionMapper
     *            callback class for handling mappings of locations and regions
     */
    protected void addLocationsToRegionArray(TIntHashSet locs,
            Gazetteer gaz) {
        for (int locid : locs.toArray()) {
            Location loc = gaz.getLocation(locid);
            addLocationsToRegionArray(loc);
        }
    }

    /**
     * Add a single location to the Region object in the region array that
     * corresponds to the latitude and longitude stored in the location object.
     * Create the Region object if necessary. Uses `regionMapper' to actually
     * create the location.
     * 
     * @param loc
     * @param regionMapper
     */
    protected void addLocationsToRegionArray(Location loc) {
        int curX = (int) (loc.getCoord().latitude + 180)
                / (int) degreesPerRegion;
        int curY = (int) (loc.getCoord().longitude + 90)
                / (int) degreesPerRegion;
        // System.out.println(loc.coord.latitude + ", " + loc.coord.longitude +
        // " goes to");
        // System.out.println(curX + " " + curY);
        if (curX < 0 || curY < 0) {
            if (curX < 0) {
                curX = 0;
            }
            if (curY < 0) {
                curY = 0;
            }
            System.err.println("Warning: " + loc.getName() + " had invalid coordinates (" + loc.getCoord() + "); mapping it to [" + curX + "][" + curY + "]");
        }
        if (regionArray[curX][curY] == null) {
            double minLon = loc.getCoord().longitude - loc.getCoord().longitude % degreesPerRegion;
            double maxLon = minLon + (loc.getCoord().longitude < 0 ? -1 : 1) * degreesPerRegion;
            double minLat = loc.getCoord().latitude - loc.getCoord().latitude % degreesPerRegion;
            double maxLat = minLat + (loc.getCoord().latitude < 0 ? -1 : 1) * degreesPerRegion;
            regionArray[curX][curY] = new Region(minLon, maxLon, minLat, maxLat);
            activeRegions++;
        }
    }

    /**
     * Process training data. Populate lexicon, identify toponyms, build arrays.
     *
     * @throws Exception
     */
    public void processTrainInputPath() throws Exception {
        trainTokenArrayBuffer = new TokenArrayBuffer(lexicon);
        System.out.println("Warning: code is broken! Can't process " + trainInputFile.getCanonicalPath());
        // processTrainInputPath(trainInputFile, new TextProcessor(lexicon, paragraphsAsDocs), trainTokenArrayBuffer);
        trainTokenArrayBuffer.convertToPrimitiveArrays();
    }

    /**
     * 
     * @param myPath
     * @param textProcessor
     * @param tokenArrayBuffer
     * @param stopwordList
     * @throws IOException
     */
    public void processTrainInputPath(File myPath, TextProcessor textProcessor,
          TokenArrayBuffer tokenArrayBuffer) throws
          IOException {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processTrainInputPath(new File(myPath.getCanonicalPath() + File.separator + pathname), textProcessor, tokenArrayBuffer);
            }
        } else {
            System.err.println("Warning: code is broken! Can't process " + myPath.getCanonicalPath());
            // textProcessor.processFile(myPath.getCanonicalPath(), tokenArrayBuffer);
        }
    }

    /**
     * Process training data. Populate lexicon, identify toponyms, build arrays.
     *
     * @throws Exception
     */
    public void processEvalInputPath() throws Exception {
        evalTokenArrayBuffer = new EvalTokenArrayBuffer(lexicon);
        System.err.println("Warning: code is broken! Can't process " + evalInputFile.getCanonicalPath());
        // processEvalInputPath(evalInputFile, new TextProcessorTR(lexicon), evalTokenArrayBuffer);
        evalTokenArrayBuffer.convertToPrimitiveArrays();
    }

    /**
     *
     * @param myPath
     * @param textProcessor
     * @param tokenArrayBuffer
     * @param stopwordList
     * @throws IOException
     */
    public void processEvalInputPath(File myPath, TextProcessor textProcessor,
          EvalTokenArrayBuffer evalTokenArrayBuffer)
          throws
          IOException {
        if (myPath.isDirectory()) {
            for (String pathname : myPath.list()) {
                processEvalInputPath(new File(myPath.getCanonicalPath() + File.separator + pathname), textProcessor, evalTokenArrayBuffer);
            }
        } else {
            System.err.println("Warning: code is broken! Can't process " + myPath.getCanonicalPath());
            // textProcessor.processFile(myPath.getCanonicalPath(), evalTokenArrayBuffer);
        }
    }

    /**
     * Output tagged and disambiguated placenames to Google Earth kml file.
     *
     * @param trainInputPath
     * @param kmlOutputFilename
     * @param locations
     * @throws Exception
     */
    public void writeXMLFile(String inputFilename, String outputFilename,
          TIntObjectHashMap<Location> idxToLocationMap, TIntHashSet locations,
          TokenArrayBuffer tokenArrayBuffer) throws
          IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));
        int dotKmlIndex = outputFilename.lastIndexOf(".kml");
        String contextFilename = outputFilename.substring(0, dotKmlIndex) + "-context.kml";
        BufferedWriter contextOut = new BufferedWriter(new FileWriter(contextFilename));

        out.write(KMLUtil.genKMLHeader(inputFilename));

        contextOut.write(KMLUtil.genKMLHeader(inputFilename));

        for (TIntIterator it = locations.iterator(); it.hasNext();) {
            int locid = it.next();
            Location loc = idxToLocationMap.get(locid);

            double height = Math.log(loc.getCount()) * barScale;

            double radius = .15;
            //String kmlPolygon = coord.toKMLPolygon(4,radius,height);  // a square
            String kmlPolygon = loc.getCoord().toKMLPolygon(10, radius, height);

            String placename = null;
            try {
                placename = loc.getName();
            } catch (UnsupportedOperationException e) {
                placename = lexicon.getWordForInt(loc.getNameid());
            }
            Coordinate coord = loc.getCoord();
            out.write(KMLUtil.genPolygon(placename, coord, radius, kmlPolygon));

            for (int j = 0; j < loc.getBackPointers().size(); j++) {
                int index = loc.getBackPointers().get(j);
                String context = tokenArrayBuffer.getContextAround(index, windowSize, true);
                Coordinate spiralPoint = coord.getNthSpiralPoint(j, 0.13);

                contextOut.write(KMLUtil.genSpiralpoint(placename, context, spiralPoint, j, radius));
            }
        }

        out.write(KMLUtil.genKMLFooter());
        contextOut.write(KMLUtil.genKMLFooter());

        out.close();
        contextOut.close();
    }

    public void evaluate() {
        evaluate(evalTokenArrayBuffer);
    }

    /**
     * 
     * @param evalTokenArrayBuffer
     */
    public void evaluate(EvalTokenArrayBuffer evalTokenArrayBuffer) {
        if (evalTokenArrayBuffer.modelLocationArrayList.size() != evalTokenArrayBuffer.goldLocationArrayList.size()) {
            System.out.println("MISMATCH: model: " + evalTokenArrayBuffer.modelLocationArrayList.size() + "; gold: " + evalTokenArrayBuffer.goldLocationArrayList.size());
            /*	    for (int i = 0; i < evalTokenArrayBuffer.size(); i++) {
            Location curModelLoc = evalTokenArrayBuffer.modelLocationArrayList.get(i);
            Location curGoldLoc = evalTokenArrayBuffer.goldLocationArrayList.get(i);
            if(curModelLoc != null && curGoldLoc != null && curMode
            }*/
            System.exit(1);
        }

        int tp = 0;
        int fp = 0;
        int fn = 0;

        int goldLocationCount = 0;
        int modelLocationCount = 0;

        int noModelGuessCount = 0;
        //int noGoldCount = 0;

        // these correspond exactly with Jochen's thesis:
        int t_n = 0;
        int t_c = 0;
        int t_i = 0;
        int t_u = 0;

        try {

            BufferedWriter errorDump = new BufferedWriter(new FileWriter("error-dump.txt"));

            for (int i = 0; i < evalTokenArrayBuffer.size(); i++) {
                Location curModelLoc = evalTokenArrayBuffer.modelLocationArrayList.get(i);
                Location curGoldLoc = evalTokenArrayBuffer.goldLocationArrayList.get(i);

                if (curGoldLoc != null) {
                    goldLocationCount++;
                    t_n++;
                    if (curModelLoc != null) {
                        modelLocationCount++;
                        if (curGoldLoc.looselyMatches(curModelLoc, 1.0)) {
                            tp++;
                            t_c++;
                            //System.out.println("t_c | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                            //                       + curModelLoc.getName() + " (" + curModelLoc.getCoord() + ") p = " + curModelLoc.getPop());
                            errorDump.write("t_c | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                                            + curModelLoc.getName() + " (" + curModelLoc.getCoord() + ") p = " + curModelLoc.getPop() + "\n");
                        } else {
                            fp++;
                            fn++; // reinstated
                            t_i++;
                            //if(this instanceof PopulationBaselineModel) {
                                //System.out.println("t_i | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                                //                   + curModelLoc.getName() + " (" + curModelLoc.getCoord() + ") p = " + curModelLoc.getPop());
                                //}
                                errorDump.write("t_i | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                                            + curModelLoc.getName() + " (" + curModelLoc.getCoord() + ") p = " + curModelLoc.getPop() + "\n");
                        }
                    } else {
                        fn++;
                        t_u++;
                        //if(this instanceof PopulationBaselineModel) {
                            //System.out.println("t_u | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                            //                   + curModelLoc);
                            errorDump.write("t_c | Gold: " + curGoldLoc.getName() + " (" + curGoldLoc.getCoord() + ") | Model: "
                                            + curModelLoc + "\n");
                            //}
                        noModelGuessCount++;
                    }
                } /*else {
                    if (curModelLoc != null) {
                        //fp++; // curGoldLoc is null, which we shouldn't be punished for. Now treating these as non-toponyms.
                        noGoldCount++;
                    } else {
                        //tn++;
                    }
                }*/
                if(i < evalTokenArrayBuffer.size()-1 && evalTokenArrayBuffer.documentVector[i] != evalTokenArrayBuffer.documentVector[i+1]) {
                    //System.out.println("#############################################");
                    errorDump.write("#############################################\n");
                }
            }

            errorDump.close();

        } catch(Exception e) {
            e.printStackTrace();
        }


        /*
        double precision = (double) tp / (tp + fp);
        double recall = (double) tp / (tp + fn);
        */

        // equivalent, but maybe simpler:
        /*
        double precision = (double) tp / modelLocationCount;
        double recall = (double) tp / goldLocationCount;
        */

        // in jochen's terms:
        double precision = (double) t_c / (t_c + t_i);
        double recall = (double) t_c / (t_n); 

        double f1 = 2 * ((precision * recall) / (precision + recall));

        System.out.println("TP: " + tp);
        System.out.println("FP: " + fp);
        System.out.println("FN: " + fn);
        System.out.println();
        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F-score: " + f1);
        System.out.println();
        System.out.println("The model abstained on " + noModelGuessCount + " toponyms where there was a gold label given.");
        //System.out.println("The gold standard had no correct location labeled for " + noGoldCount + " toponyms.");
    }

    public void serializeEvalTokenArrayBuffer(String filename) throws Exception {
        System.out.print("\nSerializing evaluation token array buffer to " + filename + ".gz ...");
        ObjectOutputStream evalTokenArrayBufferOut =
              new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(filename + ".gz")));
        evalTokenArrayBufferOut.writeObject(evalTokenArrayBuffer);
        evalTokenArrayBufferOut.close();
        System.out.println("done.");
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

    class PCLXMLFilter implements FilenameFilter {

        @Override
        public boolean accept(File dir, String name) {
            return (name.startsWith("txu") && name.endsWith(".xml"));
        }
    }
}
