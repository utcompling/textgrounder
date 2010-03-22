package opennlp.textgrounder.geo;

import java.io.*;
import java.util.*;

/**
 * Extension of DocumentSet for handling paragraphs as boundaries. To avoid
 * degeneracy, paragraphs as documents do not overlap.
 *
 * @author tsmoon
 */
public class ParagraphSet extends DocumentSet {

    protected int parAsDocSize;

    public ParagraphSet() {
        super();
    }

    public ParagraphSet(int parAsDocSize) {
        this.parAsDocSize = parAsDocSize;
    }

    @Override
    public void addDocumentFromFile(String locationOfFile) throws Exception {

        BufferedReader textIn = new BufferedReader(new FileReader(locationOfFile));

        System.out.print("Processing document as multiple documents of paragraphs at "
              + locationOfFile + " ...");

        ArrayList<Integer> curDoc = new ArrayList<Integer>();
        this.add(curDoc);

        int counter = 1;
        String curLine;
        while (true) {
            curLine = textIn.readLine();
            if (curLine == null || curLine.equals("")) {
                break;
            }

            for (String token : curLine.split(" ")) {
                if (wordsToInts.containsKey(token)) {
                    curDoc.add(wordsToInts.get(token));
                } else {
                    wordsToInts.put(token, nextInt);
                    intsToWords.put(nextInt, token);
                    curDoc.add(nextInt);
                    nextInt++;
                }
            }

            if (counter < parAsDocSize) {
                counter++;
            } else {
                counter = 1;
                curDoc = new ArrayList<Integer>();
                this.add(curDoc);
            }

        }

        System.out.println("done.");

        textIn.close();
    }
}
