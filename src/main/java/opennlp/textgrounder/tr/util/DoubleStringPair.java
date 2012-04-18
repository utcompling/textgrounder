///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.tr.util;

/**
 * A pair of an double and String
 *
 * @author  Taesun Moon
 */
public final class DoubleStringPair implements Comparable<DoubleStringPair> {
    /**
     *
     */
    public double doubleValue;
    /**
     * 
     */
    public String stringValue;

    /**
     * 
     *
     * @param d
     * @param s
     */
    public DoubleStringPair (double d, String s) {
	doubleValue = d;
	stringValue = s;
    }

    /**
     * sorting order is reversed -- higher (int) values come first
     * 
     * @param p
     * @return
     */
    public int compareTo (DoubleStringPair p) {
	if (doubleValue < p.doubleValue)
	    return 1;
	else if (doubleValue > p.doubleValue)
	    return -1;
	else 
	    return 0;
    }

}
