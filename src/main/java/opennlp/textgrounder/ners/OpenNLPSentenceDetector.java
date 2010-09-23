///////////////////////////////////////////////////////////////////////////////
// Copyright (C) 2004 Jason Baldridge, Gann Bierner and Tom Morton
// Copyright (C) 2010 Ben Wing
// 
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
// 
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public
// License along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//////////////////////////////////////////////////////////////////////////////

// package opennlp.tools.lang.english;
package opennlp.textgrounder.ners;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;

import opennlp.maxent.io.SuffixSensitiveGISModelReader;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

/**
 * A sentence detector which uses a model trained on English data. The code in
 * this class is based on the code in SentenceDetector.java in package
 * opennlp.tools.lang.english.
 * 
 * @author Jason Baldridge and Tom Morton (original)
 * @author Ben Wing (adapted for use in TextGrounder)
 * @version Original version: $Revision: 1.5.2.1 $, $Date: 2008/08/31 00:35:46 $
 */

public class OpenNLPSentenceDetector extends SentenceDetectorME {
    /**
     * Loads a new sentence detector using the model specified by the model
     * name.
     * 
     * @param modelName
     *            The name of the maxent model trained for sentence detection.
     * @throws IOException
     *             If the model specified can not be read.
     */
    public OpenNLPSentenceDetector(String modelName) throws IOException {
        //super((new SuffixSensitiveGISModelReader(new File(modelName)))
        //        .getModel());
        super(new SentenceModel(new FileInputStream(new File(modelName))));
        this.useTokenEnd = true;
    }

    /**
     * Perform sentence detection the given text. On input, an empty line will
     * be treated as a paragraph boundary. Returns a string with each sentence
     * on a separate line.
     */
    public static String run(String modelName, String text) throws IOException {
        SentenceDetectorME sdetector = new OpenNLPSentenceDetector(modelName);
        StringBuffer para = new StringBuffer();
        StringBuffer out = new StringBuffer();
        BufferedReader inReader = new BufferedReader(new StringReader(text));
        for (String line = inReader.readLine(); line != null; line = inReader
                .readLine()) {
            if (line.equals("")) {
                if (para.length() != 0) {
                    // System.err.println(para.toString());
                    String[] sents = sdetector.sentDetect(para.toString());
                    for (int si = 0, sn = sents.length; si < sn; si++) {
                        out.append(sents[si]).append("\n");
                    }
                }
                out.append("\n");
                para.setLength(0);
            } else {
                para.append(line).append(" ");
            }
        }
        if (para.length() != 0) {
            String[] sents = sdetector.sentDetect(para.toString());
            for (int si = 0, sn = sents.length; si < sn; si++) {
                out.append(sents[si]).append("\n");
            }
        }
        return out.toString();
    }
}
