/*
 * Evaluator that uses signatures around each gold and predicted toponym to be used in the computation of P/R/F.
 * This Evaluator doesn't assume gold named entities.
 */

package opennlp.textgrounder.eval;

import opennlp.textgrounder.text.*;
import java.util.*;

public class SignatureEvaluator extends Evaluator {

    private static final int CONTEXT_WINDOW_SIZE = 20;

    public SignatureEvaluator(Corpus corpus) {
        super(corpus);
    }

    @Override
    public Report evaluate() {
        return null;
    }

    @Override
    public Report evaluate(Corpus<Token> pred, boolean useSelected) {
        
        Report report = new Report();

        Map<String, Integer> goldLocs = new HashMap<String, Integer>();

        for(Document<Token> doc : corpus) {
            for(Sentence<Token> sent : doc) {
                StringBuffer sb = new StringBuffer();
                List<Integer> toponymStarts = new ArrayList<Integer>();
                List<Integer> goldIdxs = new ArrayList<Integer>();
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        if(toponym.hasGold()) {
                            toponymStarts.add(sb.length());
                            goldIdxs.add(toponym.getGoldIdx());
                        }
                    }
                    sb.append(token.getForm().replaceAll("[^a-z0-9]", ""));
                }
                for(int i = 0; i < toponymStarts.size(); i++) {
                    int toponymStart = toponymStarts.get(i);
                    int goldIdx = goldIdxs.get(i);
                    goldLocs.put(getSignature(sb, toponymStart, CONTEXT_WINDOW_SIZE), goldIdx);
                }
            }
        }

        
        Map<String, Integer> predLocs = new HashMap<String, Integer>();

        for(Document<Token> doc : pred) {
            for(Sentence<Token> sent : doc) {
                StringBuffer sb = new StringBuffer();
                List<Integer> toponymStarts = new ArrayList<Integer>();
                List<Integer> predIdxs = new ArrayList<Integer>();
                for(Token token : sent) {
                    if(token.isToponym()) {
                        Toponym toponym = (Toponym) token;
                        if(toponym.hasSelected()) {
                            toponymStarts.add(sb.length());
                            predIdxs.add(toponym.getSelectedIdx());
                        }
                    }
                    sb.append(token.getForm().replaceAll("[^a-z0-9]", ""));
                }
                for(int i = 0; i < toponymStarts.size(); i++) {
                    int toponymStart = toponymStarts.get(i);
                    int predIdx = predIdxs.get(i);
                    predLocs.put(getSignature(sb, toponymStart, CONTEXT_WINDOW_SIZE), predIdx);
                }
            }
        }

        for(String context : goldLocs.keySet()) {
            if(predLocs.containsKey(context)) {
                if(goldLocs.get(context) == predLocs.get(context)) {
                    //System.out.println("TP: " + context + "|" + goldLocs.get(context));
                    report.incrementTP();
                }
                else {
                    //System.out.println("FP and FN: " + context + "|" + goldLocs.get(context) + " vs. " + predLocs.get(context));
                    //report.incrementFP();
                    //report.incrementFN();
                    report.incrementFPandFN();
                }
            }
            else {
                //System.out.println("FN: " + context + "| not found in pred");
                report.incrementFN();
            }
        }
        for(String context : predLocs.keySet()) {
            if(goldLocs.containsKey(context)) {
                if(predLocs.get(context) != goldLocs.get(context)) {
                    //System.out.println("FP: " + context + "| not found in gold");
                    report.incrementFP();
                }
            }
        }

        return report;
    }

    private String getSignature(StringBuffer wholeContext, int centerIndex, int windowSize) {
        int beginIndex = Math.max(0, centerIndex - windowSize);
        int endIndex = Math.min(wholeContext.length(), centerIndex + windowSize);

        return wholeContext.substring(beginIndex, endIndex);
    }
}
