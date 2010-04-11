///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.ners;

import edu.stanford.nlp.ie.crf.*;

/**
 * An override of the CRFClassifier for use in the base TopicModel which
 * does not need placename identification.
 * 
 * @author tsmoon
 */
public class NullClassifier extends CRFClassifier {

    /**
     * Default constructor. Does nothing.
     */
    public NullClassifier() {
    }

    /**
     * Override of sole CRFClassifier method used in TextProcessor. Merely
     * lowercases words and strips tokens of non-alphanumeric characters
     *
     * @param text string to process
     * @return input string lowercased and removed of all non-alphanumeric
     * characters
     */
    @Override
    public String classifyToString(String text) {
        StringBuffer buf = new StringBuffer();
        String[] tokens = text.split(" ");
        for (String token : tokens) {
            buf.append(token.toLowerCase().replaceAll("[^a-z0-9]", ""));
            buf.append(" ");
        }
        return buf.toString();
    }
}
