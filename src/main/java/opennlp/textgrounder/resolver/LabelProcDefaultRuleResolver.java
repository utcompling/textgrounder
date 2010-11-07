package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.*;

public class LabelProcDefaultRuleResolver extends Resolver {

    public static final int DEGREES_PER_REGION = 1;

    private String pathToGraph;
    private Lexicon<String> lexicon = new SimpleLexicon<String>();
    private HashMap<Integer, String> reverseLexicon = new HashMap<Integer, String>();
    private HashMap<Integer, Integer> defaultRegions = null;
    private HashMap<Integer, Integer> indexCache = new HashMap<Integer, Integer>();

    public LabelProcDefaultRuleResolver(String pathToGraph) {
        this.pathToGraph = pathToGraph;
    }

    @Override
    public void train(StoredCorpus corpus){
        buildLexicons(corpus, lexicon, reverseLexicon);

        defaultRegions = new HashMap<Integer, Integer>();

        try {
            BufferedReader in = new BufferedReader(new FileReader(pathToGraph));

            String curLine;
            while(true) {
                curLine = in.readLine();
                if(curLine == null)
                    break;

                String[] tokens = curLine.split("\t");

                int idx = Integer.parseInt(tokens[0]);

                if(!reverseLexicon.containsKey(idx))
                    continue;

                int regionNumber = -1;
                for(int i = 1; i < tokens.length; i++) {
                    String curToken = tokens[i];
                    if(curToken.length() == 0)
                        continue;

                    String[] innerTokens = curToken.split(" ");
                    for(int j = 0; j < innerTokens.length; j++) {
                        if(/*!innerTokens[j].startsWith("__DUMMY__") && */innerTokens[j].endsWith("L")) {
                            regionNumber = Integer.parseInt(innerTokens[j].substring(0, innerTokens[j].length()-1));
                            break;
                        }
                    }
                }

                if(regionNumber == -1)
                    continue;

                defaultRegions.put(idx, regionNumber);
            }

            in.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        if(defaultRegions == null)
            train(corpus);

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int idx = lexicon.get(toponym.getForm());
                        Integer indexToSelect = indexCache.get(idx);
                        if(indexToSelect == null) {
                            int regionNumber = defaultRegions.get(idx);
                            indexToSelect = getCorrectCandidateIndex(toponym, regionNumber);
                            indexCache.put(idx, indexToSelect);
                        }
                        //System.out.println("index selected for " + toponym.getForm() + ": " + indexToSelect);
                        if(indexToSelect != -1)
                            toponym.setSelectedIdx(indexToSelect);
                    }
                }
            }
        }

        return corpus;
    }

    private int getCorrectCandidateIndex(Toponym toponym, int regionNumber) {

        //System.out.println(toponym.getForm() + " (" + lexicon.get(toponym.getForm()) + ")");

        int index = 0;
        for(Location location : toponym.getCandidates()) {
            //System.out.println(getRegionNumber(location) + " vs. " + regionNumber);
            if(getRegionNumber(location) == regionNumber)
                return index;
            index++;
        }

        return -1;
    }

    private int getRegionNumber(Location location) {
        int x = (int) ((location.getRegion().getCenter().getLng() + 180.0) / DEGREES_PER_REGION);
        int y = (int) ((location.getRegion().getCenter().getLat() + 90.0) / DEGREES_PER_REGION);

        return x * 1000 + y;
    }

    private void buildLexicons(StoredCorpus corpus, Lexicon<String> lexicon, HashMap<Integer, String> reverseLexicon) {
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

}
