package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.topo.gaz.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.*;
import java.util.zip.*;

public class LabelPropPreproc {

    public static final int DEGREES_PER_REGION = 1;
    private static final double REGION_REGION_WEIGHT = 0.9;

    private static final double WORD_WORD_WEIGHT_THRESHOLD = 0.0;

    private static final int IN_DOC_COUNT_THRESHOLD = 5;
    private static final int MATRIX_COUNT_THRESHOLD = 2;

    private static final String ARTICLE_TITLE = "Article title: ";
    private static final String ARTICLE_ID = "Article ID: ";

    private static int toponymLexiconSize;

    // arg0: path to corpus
    // arg1: path to graph file
    // arg2: path to seed file
    // arg3: path to serialized Geonames gazetteer ([name].ser.gz)
    // arg4: path to wiki file (article titles, article IDs, and word lists; as output by Ben's wikipedia processing code)
    // arg5: path to stoplist filename (one stopword per line)
    public static void main(String[] args) throws Exception {

        Tokenizer tokenizer = new OpenNLPTokenizer();
        OpenNLPRecognizer recognizer = new OpenNLPRecognizer();

        System.out.println("Reading serialized GeoNames gazetteer from " + args[3] + " ...");
        GZIPInputStream gis = new GZIPInputStream(new FileInputStream(args[3]));
        ObjectInputStream ois = new ObjectInputStream(gis);
        GeoNamesGazetteer gnGaz = (GeoNamesGazetteer) ois.readObject();
        System.out.println("Done.");

        StoredCorpus corpus = Corpus.createStoredCorpus();
        System.out.print("Reading TR-CoNLL corpus from " + args[0] + " ...");
        //corpus.addSource(new TrXMLDirSource(new File(args[0]), tokenizer));
        corpus.addSource(new ToponymAnnotator(new ToponymRemover(new TrXMLDirSource(new File(args[0]), tokenizer)), recognizer, gnGaz));
        corpus.load();
        System.out.println("done.");

        Map<Integer, Set<Integer> > toponymRegionEdges = new HashMap<Integer, Set<Integer> >();

        Lexicon<String> toponymLexicon = TopoUtil.buildLexicon(corpus);
        toponymLexiconSize = toponymLexicon.size();

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int idx = toponymLexicon.get(toponym.getForm());
                        Set<Integer> regionSet = toponymRegionEdges.get(idx);
                        if(regionSet == null) {
                            regionSet = new HashSet<Integer>();
                            for(Location location : toponym.getCandidates()) {
                                //int regionNumber = TopoUtil.getRegionNumbers(location, DEGREES_PER_REGION);
                                //regionSet.add(regionNumber);
                                regionSet.addAll(TopoUtil.getRegionNumbers(location, DEGREES_PER_REGION));
                            }
                            toponymRegionEdges.put(idx, regionSet);
                        }
                    }
                }
            }
        }

        writeToponymRegionEdges(toponymRegionEdges, args[1]);
        writeRegionRegionEdges(args[1]);
        writeWordWordEdges(toponymLexicon, args[4], args[1], args[5]);

        writeRegionLabels(toponymRegionEdges, args[2]);
    }

    private static Set<String> buildStoplist(String stoplistFilename) throws Exception {
        Set<String> stoplist = new HashSet<String>();

        BufferedReader in = new BufferedReader(new FileReader(stoplistFilename));

        String curLine;
        while(true) {
            curLine = in.readLine();
            if(curLine == null)
                break;

            if(curLine.length() > 0)
                stoplist.add(curLine.toLowerCase());
        }

        in.close();

        return stoplist;
    }

    private static void writeWordWordEdges(Lexicon<String> lexicon, String wikiFilename,
            String outputFilename, String stoplistFilename) throws Exception {

        Set<String> stoplist = buildStoplist(stoplistFilename);

        int docCount = 0;
        Map<Integer, Map<Integer, Integer> > countMatrix = new HashMap<Integer, Map<Integer, Integer> >();

        BufferedReader wikiIn = new BufferedReader(new FileReader(wikiFilename));

        boolean skip = true;

        String curLine;
        String articleTitle = null;
        Set<Integer> wordsInDoc = null;
        while(true) {
            curLine = wikiIn.readLine();
            if(curLine == null)
                break;

            if(curLine.startsWith(ARTICLE_ID)) {
                System.err.println(curLine + (skip?" skipped":""));
                continue;
            }

            if(curLine.startsWith(ARTICLE_TITLE)) {
                if(wordsInDoc != null && wordsInDoc.size() > 0) {
                    for(Integer i1 : wordsInDoc) {
                        Map<Integer, Integer> curMap = countMatrix.get(i1);
                        if(curMap == null) {
                            curMap = new HashMap<Integer, Integer>();
                        }
                        for(Integer i2 : wordsInDoc) {
                            Integer curCount = curMap.get(i2);
                            if(curCount == null) {
                                curCount = 0;
                            }
                            curMap.put(i2, curCount + 1);
                        }
                        countMatrix.put(i1, curMap);
                    }

                    docCount++;
                }

                articleTitle = curLine.substring(ARTICLE_TITLE.length()).trim().toLowerCase();
                if(lexicon.contains(articleTitle)) {
                    skip = false;
                    wordsInDoc = new HashSet<Integer>();
                }
                else
                    skip = true;
            }

            else if(!skip) {
                //System.err.println(curLine);

                String[] tokens = curLine.split(" ");
                String word = tokens[0].toLowerCase();

                if(!stoplist.contains(word) && Integer.parseInt(tokens[2]) >= IN_DOC_COUNT_THRESHOLD) {
                    wordsInDoc.add(lexicon.getOrAdd(word));
                }
                //System.err.println(curLine);
            }

        }

        //System.err.println(countMatrix.get(17).get(17));

        wikiIn.close();

        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("lexicon.ser"));
        oos.writeObject(lexicon);
        oos.close();

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename, true));

        for(int i1 : countMatrix.keySet()) {
            Map<Integer, Integer> innerMap = countMatrix.get(i1);
            double i1count = innerMap.get(i1);
            if(i1count < MATRIX_COUNT_THRESHOLD) continue;
            for(int i2 : innerMap.keySet()) {
                if(i1 != i2) {

                    double i2count = countMatrix.get(i2).get(i2);
                    double i1i2count = innerMap.get(i2);

                    if(i1i2count < MATRIX_COUNT_THRESHOLD) continue;

                    double probi1 = i1count / docCount;
                    double probi2 = i2count / docCount;
                    double probi1i2 = i1i2count / docCount;

                    /*System.err.println(i1);
                    System.err.println(i2);
                    System.err.println(docCount);

                    System.err.println(probi1);
                    System.err.println(probi2);
                    System.err.println(probi1i2);*/

                    double wordWordWeight = Math.log(probi1i2 / (probi1 * probi2));

                    /*System.err.println(pmi);
                    System.err.println("---");*/

                    
                    if(wordWordWeight > WORD_WORD_WEIGHT_THRESHOLD)
                        out.write(i1 + "\t" + i2 + "\t" + wordWordWeight + "\n");
                }
            }
        }

        out.close();
    }

    private static void writeToponymRegionEdges(Map<Integer, Set<Integer> > toponymRegionEdges, String filename) throws Exception {
        BufferedWriter out = new BufferedWriter(new FileWriter(filename));

        for(int idx : toponymRegionEdges.keySet()) {
            int size = toponymRegionEdges.get(idx).size();
            double weight = 1.0/size;
            for(int regionNumber : toponymRegionEdges.get(idx)) {
                out.write(idx + "\t" + regionNumber + "R\t" + weight + "\n");
                out.write(regionNumber + "R\t" + idx + "\t1.0\n");
            }
        }

        out.close();
    }

    private static void writeRegionRegionEdges(String filename) throws Exception {
        BufferedWriter out = new BufferedWriter(new FileWriter(filename, true));

        for(int lon = 0; lon < 360 / DEGREES_PER_REGION; lon += DEGREES_PER_REGION) {
            for(int lat = 0; lat < 180 / DEGREES_PER_REGION; lat += DEGREES_PER_REGION) {
                int curRegionNumber = TopoUtil.getRegionNumber(lat, lon, DEGREES_PER_REGION);
                int leftRegionNumber = TopoUtil.getRegionNumber(lat, lon - DEGREES_PER_REGION, DEGREES_PER_REGION);
                int rightRegionNumber = TopoUtil.getRegionNumber(lat, lon + DEGREES_PER_REGION, DEGREES_PER_REGION);
                int topRegionNumber = TopoUtil.getRegionNumber(lat + DEGREES_PER_REGION, lon, DEGREES_PER_REGION);
                int bottomRegionNumber = TopoUtil.getRegionNumber(lat - DEGREES_PER_REGION, lon, DEGREES_PER_REGION);

                out.write(curRegionNumber + "R\t" + leftRegionNumber + "R\t" + REGION_REGION_WEIGHT + "\n");
                out.write(curRegionNumber + "R\t" + rightRegionNumber + "R\t" + REGION_REGION_WEIGHT + "\n");
                if(topRegionNumber >= 0)
                    out.write(curRegionNumber + "R\t" + topRegionNumber + "R\t" + REGION_REGION_WEIGHT + "\n");
                if(bottomRegionNumber >= 0)
                    out.write(curRegionNumber + "R\t" + bottomRegionNumber + "R\t" + REGION_REGION_WEIGHT + "\n");
            }
        }

        out.close();
    }

    private static void writeRegionLabels(Map<Integer, Set<Integer> > toponymRegionEdges, String filename) throws Exception {
        BufferedWriter out = new BufferedWriter(new FileWriter(filename));

        Set<Integer> uniqueRegionNumbers = new HashSet<Integer>();

        for(int idx : toponymRegionEdges.keySet()) {
            for(int regionNumber : toponymRegionEdges.get(idx)) {
                uniqueRegionNumbers.add(regionNumber);
            }
        }

        for(int regionNumber : uniqueRegionNumbers) {
            out.write(regionNumber + "R\t" + regionNumber + "L\t1.0\n");
        }

        out.close();
    }
}
