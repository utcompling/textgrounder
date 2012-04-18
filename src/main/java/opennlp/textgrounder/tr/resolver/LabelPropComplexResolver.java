package opennlp.textgrounder.tr.resolver;

import opennlp.textgrounder.tr.text.*;
import opennlp.textgrounder.tr.topo.*;
import opennlp.textgrounder.tr.util.*;
import opennlp.textgrounder.tr.app.*;
import java.io.*;
import java.util.*;

public class LabelPropComplexResolver extends Resolver {

    public static final double DPC = 1.0; // degrees per cell

    private String pathToGraph;
    private Map<String, Map<Integer, Double> > cellDistributions = null;

    public LabelPropComplexResolver(String pathToGraph) {
        this.pathToGraph = pathToGraph;
    }

    @Override
    public void train(StoredCorpus corpus) {
        cellDistributions = new HashMap<String, Map<Integer, Double> >();

        try {
            BufferedReader in = new BufferedReader(new FileReader(pathToGraph));

            String curLine;
            while(true) {
                curLine = in.readLine();
                if(curLine == null)
                    break;

                String[] tokens = curLine.split("\t");

                if(!tokens[0].startsWith(LabelPropPreproc.DOC_) || !tokens[0].contains(LabelPropPreproc.TOK_))
                    continue;

                int docIdBeginIndex = tokens[0].indexOf(LabelPropPreproc.DOC_) + LabelPropPreproc.DOC_.length();
                int lastTOKIndex = tokens[0].lastIndexOf(LabelPropPreproc.TOK_);
                int docIdEndIndex = lastTOKIndex - 1; // - 1 for intermediary "_"
                String docId = tokens[0].substring(docIdBeginIndex, docIdEndIndex);

                String tokenIndex = tokens[0].substring(lastTOKIndex + LabelPropPreproc.TOK_.length());

                String key = docId + ";" + tokenIndex;

                Map<Integer, Double> cellDistribution = new HashMap<Integer, Double>();

                for(int i = 1; i < tokens.length; i++) {
                    String curToken = tokens[i];
                    if(curToken.length() == 0)
                        continue;

                    String[] innerTokens = curToken.split(" ");
                    for(int j = 0; j < innerTokens.length; j++) {
                        if(/*!innerTokens[j].startsWith("__DUMMY__") && */innerTokens[j].startsWith(LabelPropPreproc.CELL_LABEL_)) {
                            int cellNumber = Integer.parseInt(innerTokens[j].substring(LabelPropPreproc.CELL_LABEL_.length()));
                            double mass = Double.parseDouble(innerTokens[j+1]);
                            cellDistribution.put(cellNumber, mass);
                        }
                    }

                    cellDistributions.put(key, cellDistribution);
                }

                
            }

            in.close();
            
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public StoredCorpus disambiguate(StoredCorpus corpus) {

        if(cellDistributions == null)
            train(corpus);

        for(Document<StoredToken> doc : corpus) {
            int tokenIndex = 0;
            for(Sentence<StoredToken> sent : doc) {
                for(Toponym toponym : sent.getToponyms()) {
                    if(toponym.getAmbiguity() > 0) {
                        int indexToSelect = TopoUtil.getCorrectCandidateIndex(toponym, cellDistributions.get(doc.getId() + ";" + tokenIndex), DPC);
                        if(indexToSelect != -1)
                            toponym.setSelectedIdx(indexToSelect);
                    }
                }
                tokenIndex++;
            }
        }

        return corpus;
    }
}
