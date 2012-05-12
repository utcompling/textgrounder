package opennlp.textgrounder.tr.util;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import java.util.*;
import opennlp.textgrounder.tr.resolver.*;
import opennlp.textgrounder.tr.text.io.*;
import opennlp.textgrounder.tr.text.prep.*;
import opennlp.textgrounder.tr.topo.gaz.*;
import opennlp.textgrounder.tr.eval.*;
import opennlp.textgrounder.tr.util.*;
import java.io.*;
import java.util.zip.*;

public class TopoUtil {
    
    public static Lexicon<String> buildLexicon(StoredCorpus corpus) {
        Lexicon<String> lexicon = new SimpleLexicon<String>();

        addToponymsToLexicon(lexicon, corpus);

        return lexicon;
    }

    public static void addToponymsToLexicon(Lexicon<String> lexicon, StoredCorpus corpus) {
        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        lexicon.getOrAdd(toponym.getForm());
                    }
                }
            }
        }
    }

    public static void buildLexicons(StoredCorpus corpus, Lexicon<String> lexicon, HashMap<Integer, String> reverseLexicon) {
        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int idx = lexicon.getOrAdd(toponym.getForm());
                        reverseLexicon.put(idx, toponym.getForm());
                    }
                }
            }
        }
    }

    public static Set<Integer> getCellNumbers(Location location, double dpc) {

        Set<Integer> cellNumbers = new HashSet<Integer>();

        for(Coordinate coord : location.getRegion().getRepresentatives()) {
        
            /*int x = (int) ((coord.getLng() + 180.0) / dpc);
              int y = (int) ((coord.getLat() + 90.0) / dpc);*/

            cellNumbers.add(getCellNumber(coord.getLat(), coord.getLng(), dpc)/*x * 1000 + y*/);
        }

        return cellNumbers;
    }

    public static int getCellNumber(double lat, double lon, double dpc) {

        int x = (int) ((lon + 180.0) / dpc);
        int y = (int) ((lat + 90.0) / dpc);

        if(y < 0 || y >= 180/dpc) return -1;
        if(x < 0) x += 360/dpc;
        if(x > 360/dpc) x -= 360/dpc;

        return x * 1000 + y;

        // Make everything positive:
        /*lat += 90/dpc;
        lon += 180/dpc;

        if(lat < 0 || lat >= 180/dpc) return -1;
        if(lon < 0) lon += 360/dpc; // wrap lon around
        if(lon >= 360/dpc) lon -= 360/dpc; // wrap lon around

        return (int)((int)(lat/dpc) * 1000 + (lon/dpc));*/
    }

    public static int getCellNumber(Coordinate coord, double dpc) {
        return getCellNumber(coord.getLatDegrees(), coord.getLngDegrees(), dpc);
    }

    public static Coordinate getCellCenter(int cellNumber, double dpc) {
        int x = cellNumber / 1000;
        int y = cellNumber % 1000;

        double lat = (dpc * y - 90) + dpc/2.0;
        double lon = (dpc * x - 180) + dpc/2.0;

        if(lat >= 90) lat -= 90;
        if(lon >= 180) lon -= 180;

        return Coordinate.fromDegrees(lat, lon);
    }

    public static int getCorrectCandidateIndex(Toponym toponym, Map<Integer, Double> cellDistribution, double dpc) {
        double maxMass = Double.NEGATIVE_INFINITY;
        int maxIndex = -1;
        int index = 0;
        for(Location location : toponym.getCandidates()) {
            double totalMass = 0.0;
            for(int cellNumber : getCellNumbers(location, dpc)) {
                //if(regionDistribution == null)
                //    System.err.println("regionDistribution is null!");
                
                Double mass = cellDistribution.get(cellNumber);
                //if(mass == null)
                //    System.err.println("mass null for regionNumber " + regionNumber);
                if(mass != null)
                    totalMass += mass;
            }
            if(totalMass > maxMass) {
                maxMass = totalMass;
                maxIndex = index;
            }
            index++;
        }

        return maxIndex;
    }
    
    public static int getCorrectCandidateIndex(Toponym toponym, int cellNumber, double dpc) {
        if(cellNumber == -1) System.out.println("-1");
        int index = 0;
        for(Location location : toponym.getCandidates()) {
            if(getCellNumbers(location, dpc).contains(cellNumber))
                return index;
            index++;
        }

        return -1;
    }

    public static Corpus readCorpusFromSerialized(String serializedCorpusInputPath) throws Exception {

        Corpus corpus;
        ObjectInputStream ois = null;
        if(serializedCorpusInputPath.toLowerCase().endsWith(".gz")) {
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(serializedCorpusInputPath));
            ois = new ObjectInputStream(gis);
        }
        else {
            FileInputStream fis = new FileInputStream(serializedCorpusInputPath);
            ois = new ObjectInputStream(fis);
        }
        corpus = (StoredCorpus) ois.readObject();

        return corpus;
    }


    public static StoredCorpus readStoredCorpusFromSerialized(String serializedCorpusInputPath) throws Exception {

        StoredCorpus corpus;
        ObjectInputStream ois = null;
        if(serializedCorpusInputPath.toLowerCase().endsWith(".gz")) {
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(serializedCorpusInputPath));
            ois = new ObjectInputStream(gis);
        }
        else {
            FileInputStream fis = new FileInputStream(serializedCorpusInputPath);
            ois = new ObjectInputStream(fis);
        }
        corpus = (StoredCorpus) ois.readObject();

        return corpus;
    }

    public static List<Location> filter(List<Location> locs, Region boundingBox) {
        if(boundingBox == null || locs == null) return locs;

        List<Location> toReturn = new ArrayList<Location>();
        
        for(Location loc : locs) {
            boolean containsAllPoints = true;
            for(Coordinate coord : loc.getRegion().getRepresentatives()) {
                if(!boundingBox.contains(coord)) {
                    containsAllPoints = false;
                    break;
                }
            }
            if(containsAllPoints)
                toReturn.add(loc);
        }

        return toReturn;
    }
}
