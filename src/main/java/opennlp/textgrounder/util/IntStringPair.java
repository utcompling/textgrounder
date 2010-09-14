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
 * A pair of an int and String
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.1 $, $Date: 2005/04/14 04:22:57 $
 */
public final class IntStringPair extends TLinkableAdapter implements Comparable<IntStringPair> {
    public int intValue;
    public String stringValue;

    public IntStringPair (int i, String d) {
	intValue = i;
	stringValue = d;
    }

    // note: sorting order is reversed -- higher (int) values come first
    public int compareTo (IntStringPair p) {
	if (intValue < p.intValue)
	    return 1;
	else if (intValue > p.intValue)
	    return -1;
	else 
	    return 0;
    }

}
