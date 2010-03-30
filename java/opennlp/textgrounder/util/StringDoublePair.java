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
 *
 * @author tsmoon
 */
public class StringDoublePair implements Comparable<StringDoublePair> {

    public String stringValue;
    public double doubleValue;

    public StringDoublePair(String s, double d) {
        stringValue = s;
        doubleValue = d;
    }

    public int compareTo(StringDoublePair p) {
        return stringValue.compareTo(p.stringValue);
    }

}
