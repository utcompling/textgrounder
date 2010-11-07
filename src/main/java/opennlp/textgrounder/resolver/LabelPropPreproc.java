package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.text.io.*;
import opennlp.textgrounder.text.prep.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.*;

public class LabelPropPreproc {

    public static final int DEGREES_PER_REGION = 1;

    public static void main(String[] args) throws Exception {

        Tokenizer tokenizer = new OpenNLPTokenizer();

        StoredCorpus corpus = Corpus.createStoredCorpus();
        System.out.print("Reading corpus from " + args[0] + " ...");
        corpus.addSource(new TrXMLDirSource(new File(args[0]), tokenizer));
        corpus.load();
        System.out.println("done.");

        Map<Integer, Set<Integer> > toponymRegionEdges = new HashMap<Integer, Set<Integer> >();

        Lexicon<String> toponymLexicon = TopoUtil.buildLexicon(corpus);

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int idx = toponymLexicon.get(toponym.getForm());
                        Set<Integer> regionSet = toponymRegionEdges.get(idx);
                        if(regionSet == null) {
                            regionSet = new HashSet<Integer>();
                            for(Location location : toponym.getCandidates()) {
                                int regionNumber = getRegionNumber(location);
                                regionSet.add(regionNumber);
                            }
                            toponymRegionEdges.put(idx, regionSet);
                        }
                    }
                }
            }
        }

        writeToponymRegionEdges(toponymRegionEdges, args[1]);
        writeRegionLabels(toponymRegionEdges, args[2]);
    }

    private static void writeToponymRegionEdges(Map<Integer, Set<Integer> > toponymRegionEdges, String filename) throws Exception {
        BufferedWriter out = new BufferedWriter(new FileWriter(filename));

        for(int idx : toponymRegionEdges.keySet()) {
            int size = toponymRegionEdges.get(idx).size();
            double weight = 1.0/size;
            for(int regionNumber : toponymRegionEdges.get(idx)) {
                out.write(idx + "\t" + regionNumber + "\t" + weight + "\n");
                out.write(regionNumber + "\t" + idx + "\t1.0\n");
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
            out.write(regionNumber + "\t" + regionNumber + "L\t1.0\n");
        }

        out.close();
    }

    private static int getRegionNumber(Location location) {
        int x = (int) ((location.getRegion().getCenter().getLng() + 180.0) / DEGREES_PER_REGION);
        int y = (int) ((location.getRegion().getCenter().getLat() + 90.0) / DEGREES_PER_REGION);

        return x * 1000 + y;
    }
}
