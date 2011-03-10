package opennlp.textgrounder.resolver;

import opennlp.textgrounder.text.*;
import opennlp.textgrounder.topo.*;
import opennlp.textgrounder.util.*;
import java.io.*;
import java.util.*;

public class LabelPropDefaultRuleResolver extends Resolver {

    public static final int DEGREES_PER_REGION = 1;

    private String pathToGraph;
    private Lexicon<String> lexicon = new SimpleLexicon<String>();
    private HashMap<Integer, String> reverseLexicon = new HashMap<Integer, String>();
    //private HashMap<Integer, Integer> defaultRegions = null;
    private HashMap<Integer, HashMap<Integer, Double> > regionDistributions = null;//new HashMap<Integer, HashMap<Integer, Double> >();
    private HashMap<Integer, Integer> indexCache = new HashMap<Integer, Integer>();

    public LabelPropDefaultRuleResolver(String pathToGraph) {
        this.pathToGraph = pathToGraph;
    }

    @Override
    public void train(StoredCorpus corpus) {
        TopoUtil.buildLexicons(corpus, lexicon, reverseLexicon);

        //defaultRegions = new HashMap<Integer, Integer>();
        regionDistributions = new HashMap<Integer, HashMap<Integer, Double> >();

        try {
            BufferedReader in = new BufferedReader(new FileReader(pathToGraph));

            String curLine;
            while(true) {
                curLine = in.readLine();
                if(curLine == null)
                    break;

                String[] tokens = curLine.split("\t");

                if(tokens[0].endsWith("R")) continue;//tokens[0] = tokens[0].substring(0, tokens[0].length()-1);
                
                int idx = Integer.parseInt(tokens[0]);

                if(!reverseLexicon.containsKey(idx))
                    continue;

                HashMap<Integer, Double> regionDistribution = regionDistributions.get(idx);
                if(regionDistribution == null)
                    regionDistribution = new HashMap<Integer, Double>();

                //int regionNumber = -1;
                for(int i = 1; i < tokens.length; i++) {
                    String curToken = tokens[i];
                    if(curToken.length() == 0)
                        continue;

                    String[] innerTokens = curToken.split(" ");
                    for(int j = 0; j < innerTokens.length; j++) {
                        if(/*!innerTokens[j].startsWith("__DUMMY__") && */innerTokens[j].endsWith("L")) {
                            int regionNumber = Integer.parseInt(innerTokens[j].substring(0, innerTokens[j].length()-1));
                            double mass = Double.parseDouble(innerTokens[j+1]);
                            regionDistribution.put(regionNumber, mass);
                            //break;
                        }
                    }
                }

                regionDistributions.put(idx, regionDistribution);

                /*if(regionNumber == -1) {
                    System.out.println("-1");
                    continue;
                }*/

                //defaultRegions.put(idx, regionNumber);
            }

            in.close();
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        //if(defaultRegions == null)
        if(regionDistributions == null)
            train(corpus);

        for(Document<StoredToken> doc : corpus) {
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int idx = lexicon.get(toponym.getForm());
                        Integer indexToSelect = indexCache.get(idx);
                        if(indexToSelect == null) {
                            //int regionNumber = defaultRegions.get(idx);
                            //if(regionDistributions.get(idx) == null)
                            //    System.err.println("region dist null for " + reverseLexicon.get(idx));
                            indexToSelect = TopoUtil.getCorrectCandidateIndex(toponym, regionDistributions.get(idx), DEGREES_PER_REGION);
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
}
