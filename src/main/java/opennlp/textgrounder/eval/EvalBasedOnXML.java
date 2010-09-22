/*
 * This class evaluates the text version of TextGrounder's output to the XML gold standard files in the TR-CoNLL dev/test set.
 */

package opennlp.textgrounder.eval;


import java.io.*;
import java.util.*;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;

public class EvalBasedOnXML {

    private static final int CONTEXT_WINDOW_SIZE = 20;
    private static final int CONTEXT_SIGNATURE_WINDOW_SIZE = 20;

    private static final String ERROR_DUMP_FILENAME = "error-dump.txt";
    private static final String RECALL_OUTPUT_FILENAME = "sorted-recall-output.txt";
    private static final String RESULTS_OUT_FILENAME = "results.txt";

    private static DocumentBuilderFactory dbf;
    private static DocumentBuilder db;

    public static void main(String[] args) throws Exception {
        EvalBasedOnXML eval = new EvalBasedOnXML(args[0], args[1]);
    }

    public EvalBasedOnXML(String TRXMLPath, String modelXMLPath) throws Exception {

        dbf = DocumentBuilderFactory.newInstance();
        db = dbf.newDocumentBuilder();

        TRXMLtoSingleFile(TRXMLPath, "tr-gold.xml");
        
        doEval("tr-gold.xml", modelXMLPath);
    }

    public void doEval(String TRXMLPath, String modelXMLPath) throws Exception {
        File TRXMLPathFile = new File(TRXMLPath);
        File modelXMLPathFile = new File(modelXMLPath);

        if(TRXMLPathFile.isDirectory()) {
            System.out.println(TRXMLPath + " must not be a directory.");
            return;
        }
        if(modelXMLPathFile.isDirectory()) {
            System.out.println(modelXMLPath + " must not be a directory.");
            return;
        }

        /*if(!TRXMLPath.toLowerCase().endsWith(".xml") || !modelXMLPath.toLowerCase().endsWith(".xml")) {
            System.out.println("Both files to compare must be XML files.");
            return;
        }*/

        // these correspond exactly with Jochen's thesis:
        int t_n = 0;
        int t_c = 0;
        int t_i = 0;
        int t_u = 0;

        Document trdoc = db.parse(TRXMLPath);
        Document modeldoc = db.parse(modelXMLPath);

        NodeList trToponyms = trdoc.getChildNodes().item(0).getChildNodes();
        NodeList modelToponyms = modeldoc.getChildNodes().item(0).getChildNodes();

        HashSet<Integer> trUnmatched = getToponymSet(TRXMLPath);
        HashSet<Integer> modelUnmatched = getToponymSet(modelXMLPath);

        HashMap<String, Integer> trCounts = new HashMap<String, Integer>(); // used for recall output
        HashMap<String, Integer> modelCorrectCounts = new HashMap<String, Integer>();

        int numTopsInTR = trUnmatched.size();
        int numTopsInModel = modelUnmatched.size();

        BufferedWriter errorDump = new BufferedWriter(new FileWriter(ERROR_DUMP_FILENAME));
        BufferedWriter resultsOut = new BufferedWriter(new FileWriter(RESULTS_OUT_FILENAME));

        int prevDocId = -1;

        //int matchCount = 0;

        for(int i = 0; i < trToponyms.getLength(); i++) {

            Node TRTopN = trToponyms.item(i);

            if(!TRTopN.getNodeName().equals("toponym"))
                continue;

            Node trLocationN = TRTopN.getChildNodes().item(1);
            Location trLocation = new Location();
            trLocation.setName(TRTopN.getAttributes().getNamedItem("term").getNodeValue());
            trLocation.setCoord(new Coordinate(Double.parseDouble(trLocationN.getAttributes().getNamedItem("long").getNodeValue()),
                                                Double.parseDouble(trLocationN.getAttributes().getNamedItem("lat").getNodeValue())));
            int trDocId = Integer.parseInt(TRTopN.getAttributes().getNamedItem("did").getNodeValue());
            //System.out.println(trDocId + "  " + prevDocId);

            String curKey = trLocation.getName().toLowerCase() + "|" + trLocation.getCoord().toString();
            Integer curTrCount = trCounts.get(curKey);
            if(curTrCount == null) {
                trCounts.put(curKey, 1);
                //modelCorrectCounts.put(curKey, 0);
            }
            else {
                trCounts.put(curKey, curTrCount + 1);
            }

            if(trDocId != prevDocId)
                errorDump.write("###################### d" + trDocId + " #######################\n");
            prevDocId = trDocId;
            
            Node trContextN = TRTopN.getChildNodes().item(3);
            String trContextSignature = convertContextToSignature(trContextN.getTextContent(), CONTEXT_SIGNATURE_WINDOW_SIZE);

            for(int j = 0; j < modelToponyms.getLength(); j++) {

                Node ModelTopN = modelToponyms.item(j);

                if(!ModelTopN.getNodeName().equals("toponym"))
                    continue;

                int modelDocId = Integer.parseInt(ModelTopN.getAttributes().getNamedItem("did").getNodeValue());
                if(trDocId != modelDocId)
                    continue;

                Node modelContextN = ModelTopN.getChildNodes().item(3);
                String modelContextSignature = convertContextToSignature(modelContextN.getTextContent(), CONTEXT_SIGNATURE_WINDOW_SIZE);

                if(!trContextSignature.equals(modelContextSignature)) {
                    //System.out.println(trContextSignature + " did not match " + modelContextSignature);
                    continue;
                }

                // by this point, document IDs and context signatures match, so go ahead and compare locations:

                Node modelLocationN = ModelTopN.getChildNodes().item(1);
                Location modelLocation = new Location();
                modelLocation.setName(ModelTopN.getAttributes().getNamedItem("term").getNodeValue());
                modelLocation.setCoord(new Coordinate(Double.parseDouble(modelLocationN.getAttributes().getNamedItem("long").getNodeValue()),
						      Double.parseDouble(modelLocationN.getAttributes().getNamedItem("lat").getNodeValue())));/////////////////

                //System.out.println(trContextSignature + " matched " + modelContextSignature);
                //matchCount++;


                
                if(trLocation.looselyMatches(modelLocation, 1.0)) {
                    t_c++;
                    trUnmatched.remove(i);
                    modelUnmatched.remove(j);

                    errorDump.write("cor | Gold: " + trLocation.getName() + " (" + trLocation.getCoord() + ") | Model: "
                                            + modelLocation.getName() + " (" + modelLocation.getCoord() + ") "/*p = " + modelLocation.getPop()*/ + "\n");
                    
                    //curKey = trLocation.getName() + "|" + trLocation.getCoord().toString();
                    Integer prevCount = modelCorrectCounts.get(curKey);
                    if(prevCount == null) prevCount = 0;
                    modelCorrectCounts.put(curKey, prevCount + 1);

                }
                else {
                    t_i++;

                    errorDump.write("inc | Gold: " + trLocation.getName() + " (" + trLocation.getCoord() + ") | Model: "
                                            + modelLocation.getName() + " (" + modelLocation.getCoord() + ") "/*p = " + modelLocation.getPop()*/ + "\n");
                }
                break;
            }
            t_n++;
        }

        // in jochen's terms:
        //double precision = (double) t_c / (t_c + t_i);
        //double recall = (double) t_c / (t_n);

        double precision = (double) (numTopsInModel - modelUnmatched.size()) / numTopsInModel;
        double recall = (double) (numTopsInTR - trUnmatched.size()) / numTopsInTR;

        double f1 = 2 * ((precision * recall) / (precision + recall));

        resultsOut.write("P\tR\tF\n");
        resultsOut.write(precision + "\t" + recall + "\t" + f1 + "\n");

        System.out.println("Precision: " + precision);
        System.out.println("Recall: " + recall);
        System.out.println("F-score: " + f1);
        System.out.println();
        System.out.println("Itemized error dump written to " + ERROR_DUMP_FILENAME);
        System.out.println("Tab-delineated results written to " + RESULTS_OUT_FILENAME);
        System.out.println("Sorted recall output written to " + RECALL_OUTPUT_FILENAME);

        //System.out.println("t_n = " + t_n);
        //System.out.println("matchCount = " + matchCount);

        printSortedRecallOutput(sortByValue(trCounts), modelCorrectCounts, -1, RECALL_OUTPUT_FILENAME);
        errorDump.close();
        resultsOut.close();
    }

    private void printSortedRecallOutput(List<LocationCountPair> sortedTrCounts,
            HashMap<String, Integer> modelCorrectCounts, int topN, String outputFilename) throws Exception {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));

        if(topN != -1)
            topN = Math.min(topN, sortedTrCounts.size());
        else
            topN = sortedTrCounts.size();

        for(int i = 0; i < topN; i++) {
            String curLocation = sortedTrCounts.get(i).key;
            Integer modelCorrect = modelCorrectCounts.get(curLocation);
            if(modelCorrect == null) modelCorrect = 0;
            int denom = sortedTrCounts.get(i).value;

            //System.out.println(curLocation + ": " + modelCorrect + "/" + denom + " (" + (double)modelCorrect/denom + ")");
            out.write(curLocation + ":  " + modelCorrect + "/" + denom + "  (" + (double)modelCorrect/denom + ")\n");
        }

        out.close();
    }

    private class LocationCountPair implements Comparable {
        public String key;
        public int value;

        public LocationCountPair(String key, int value) {
            this.key = key;
            this.value = value;
        }

        public int compareTo(Object o) {
            if(!(o instanceof LocationCountPair)) {
                return -1;
            }
            LocationCountPair other = (LocationCountPair) o;
            if(this.value != other.value)
                return other.value - this.value; // descending
            else
                return this.key.compareToIgnoreCase(other.key);
        }

        @Override
        public String toString() {
            return key + "=" + value;
        }
    }

    private List<LocationCountPair> sortByValue(HashMap<String, Integer> unsorted) {
        ArrayList<LocationCountPair> toReturn = new ArrayList<LocationCountPair>();
        for(String key : unsorted.keySet()) {
            toReturn.add(new LocationCountPair(key, unsorted.get(key)));
        }
        Collections.sort(toReturn);
        return toReturn;
    }

    private HashSet<Integer> getToponymSet(String xmlPath) throws Exception {
        HashSet<Integer> toReturn = new HashSet<Integer>();

        Document doc = db.parse(xmlPath);

        NodeList toponyms = doc.getChildNodes().item(0).getChildNodes();

        for(int i = 0; i < toponyms.getLength(); i++) {

            if(!toponyms.item(i).getNodeName().equals("toponym"))
                continue;

            toReturn.add(i);
        }

        return toReturn;
    }

    public void TRXMLtoSingleFile(String TRXMLPath, String outputPath) throws Exception {
        Document outputDoc = db.newDocument();
        TRXMLtoSingleFileHelper(TRXMLPath, outputDoc);
        XMLUtil.writeDocToFile(outputDoc, outputPath);
    }

    private void TRXMLtoSingleFileHelper(String TRXMLPath, Document outputDoc) throws Exception {
        
        File TRXMLFile = new File(TRXMLPath);
        if(TRXMLFile.isDirectory()) {

            Element toponymsE = outputDoc.createElement("toponyms");

            for(String filename : TRXMLFile.list()) {
                if(filename.toLowerCase().endsWith(".xml")) {
                    addToponymsToDoc(TRXMLFile.getCanonicalPath() + File.separator + filename, outputDoc, toponymsE);
                }
            }

            outputDoc.appendChild(toponymsE);
        }
        else {
            System.out.println(TRXMLPath + " isn't a directory.");
        }
    }

    private void addToponymsToDoc(String TRXMLPath, Document outputDoc, Element toponymsE) throws Exception {
        Document inputDoc = db.parse(TRXMLPath);

        String[] allTokens = XMLUtil.getAllTokens(inputDoc);
        int tokenIndex = -1;

        NodeList sentences = inputDoc.getChildNodes().item(1).getChildNodes();

        for(int i = 0; i < sentences.getLength(); i++) {
            if(!sentences.item(i).getNodeName().equals("s"))
                continue;
            NodeList tokens = sentences.item(i).getChildNodes();
            for(int j = 0; j < tokens.getLength(); j++) {
                Node tokenNode = tokens.item(j);
                if(tokenNode.getNodeName().equals("toponym")) {
                    tokenIndex++;

                    // checks if this toponym is unresolved (selected="NoneFromList") or not really a location (selected="NotALocation")
                    if(tokenNode.getAttributes().getNamedItem("selected") != null)
                        continue;

                    Node toponymNode = outputDoc.importNode(tokenNode, false);
                    Element locationE = outputDoc.createElement("location");
                    Element contextE = outputDoc.createElement("context");

                    Node candidatesNode = tokenNode.getChildNodes().item(1);

                    NodeList candidates = candidatesNode.getChildNodes();
                    //System.out.println(candidates.getLength());
                    boolean foundSelectedLocation = false;
                    for(int k = 0; k < candidates.getLength(); k++) {
                        Node candidateNode = candidates.item(k);
                        if(candidateNode.getNodeName().equals("cand")) {
                            if(candidateNode.getAttributes().getNamedItem("selected") != null) {
                                locationE.setAttribute("lat", candidateNode.getAttributes().getNamedItem("lat").getNodeValue());
                                locationE.setAttribute("long", candidateNode.getAttributes().getNamedItem("long").getNodeValue());
                                toponymNode.appendChild(locationE);
                                foundSelectedLocation = true;
                                break;
                            }
                        }
                    }

                    if(!foundSelectedLocation) // no selected location was found
                        break;

                    contextE.setTextContent(StringUtil.join(XMLUtil.getContextWindow(allTokens, tokenIndex, CONTEXT_WINDOW_SIZE)));
                    toponymNode.appendChild(contextE);

                    toponymsE.appendChild(toponymNode);
                }
                else if(tokenNode.getNodeName().equals("w")) {
                    tokenIndex++;
                }
            }
        }
    }

    private static String convertContextToSignature(String context, int windowSize) {
        int headBeginIndex = context.indexOf("[h]");
        int headEndIndex = context.indexOf("[/h]", headBeginIndex);

        String head = context.substring(headBeginIndex + "[h]".length(), headEndIndex);

        String beforeHead = context.substring(0, headBeginIndex);
        beforeHead = beforeHead.toLowerCase();
        beforeHead = beforeHead.replaceAll("[^a-z0-9]", "");

        String afterHead = context.substring(headEndIndex + "[/h]".length());
        afterHead = afterHead.toLowerCase();
        afterHead = afterHead.replaceAll("[^a-z0-9]", "");

        head = head.toLowerCase();
        head = head.replaceAll("[^a-z0-9]", "");

        if(beforeHead.length() > windowSize)
            beforeHead = beforeHead.substring(beforeHead.length() - windowSize);
        if(afterHead.length() > windowSize)
            afterHead = afterHead.substring(0, windowSize);

        return beforeHead + head + afterHead;
    }
}
