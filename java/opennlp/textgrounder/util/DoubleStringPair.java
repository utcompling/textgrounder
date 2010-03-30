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

package opennlp.textgrounder.util;

/**
 * A pair of an double and String
 *
 * @author  Taesun Moon
 */
public final class DoubleStringPair implements Comparable<DoubleStringPair> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
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
