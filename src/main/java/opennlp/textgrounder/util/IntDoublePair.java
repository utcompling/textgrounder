///////////////////////////////////////////////////////////////////////////////
// Copyright (C) 2007 Jason Baldridge, The University of Texas at Austin
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
package opennlp.textgrounder.util;

import gnu.trove.*;

/**
 * A pair of an int and double
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.1 $, $Date: 2005/04/14 04:22:57 $
 */
public final class IntDoublePair extends TLinkableAdapter implements Comparable<IntDoublePair> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    public int intValue;
    public double doubleValue;

    public IntDoublePair (int i, double d) {
	intValue = i;
	doubleValue = d;
    }

    // note: sorting order is reversed -- higher (double) values come first
    public int compareTo (IntDoublePair p) {
	if (doubleValue < p.doubleValue)
	    return 1;
	else if (doubleValue > p.doubleValue)
	    return -1;
	else 
	    return 0;
    }

    public String toString() {
	return "("+intValue+","+doubleValue+")";
    }

}
