///////////////////////////////////////////////////////////////////////////////
// Copyright (C) 2007 Jason Baldridge, The University of Texas at Austin
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
package opennlp.textgrounder.util;

/**
 * Handy utilities for dealing with strings.
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.53 $, $Date: 2006/10/12 21:20:44 $
 */
public class StringUtil {

    public static String[] splitAtLast(char sep, String s) {
        int lastIndex = s.lastIndexOf(sep);
        String[] wordtag = {s.substring(0, lastIndex),
            s.substring(lastIndex + 1, s.length())};
        return wordtag;
    }

    public static String join(Object[] array) {
        return join(" ", array);
    }

    public static String join(String sep, Object[] array) {
        StringBuilder sb = new StringBuilder(array[0].toString());
        for (int i = 1; i < array.length; i++) {
            sb.append(sep).append(array[i].toString());
        }
        return sb.toString();
    }

    public static String join(String[] array) {
        return join(" ", array);
    }

    public static String join(String sep, String[] array) {
        StringBuilder sb = new StringBuilder(array[0]);
        for (int i = 1; i < array.length; i++) {
            sb.append(sep).append(array[i]);
        }
        return sb.toString();
    }

    public static String join(int[] array) {
        return join(" ", array);
    }

    public static String join(String sep, int[] array) {
        StringBuilder sb = new StringBuilder(Integer.toString(array[0]));
        for (int i = 1; i < array.length; i++) {
            sb.append(sep).append(array[i]);
        }
        return sb.toString();
    }

    public static String join(double[] array) {
        return join(" ", array);
    }

    public static String join(String sep, double[] array) {
        StringBuilder sb = new StringBuilder(Double.toString(array[0]));
        for (int i = 1; i < array.length; i++) {
            sb.append(sep).append(array[i]);
        }
        return sb.toString();
    }

    public static String mergeJoin(String sep, String[] a1, String[] a2) {
        if (a1.length != a2.length) {
            System.out.println("Unable to merge String arrays of different length!");
            return join(a1);
        }

        StringBuilder sb = new StringBuilder(a1[0]);
        sb.append(sep).append(a2[0]);
        for (int i = 1; i < a1.length; i++) {
            sb.append(" ").append(a1[i]).append(sep).append(a2[i]);
        }
        return sb.toString();
    }

    /**
     * <p>Joins the elements of the provided array into a single String
     * containing the provided list of elements.</p>
     *
     * <p>No delimiter is added before or after the list.
     * A <code>null</code> separator is the same as an empty String ("").
     * Null objects or empty strings within the array are represented by
     * empty strings.</p>
     *
     * <pre>
     * StringUtils.join(null, *)                = null
     * StringUtils.join([], *)                  = ""
     * StringUtils.join([null], *)              = ""
     * StringUtils.join(["a", "b", "c"], "--")  = "a--b--c"
     * StringUtils.join(["a", "b", "c"], null)  = "abc"
     * StringUtils.join(["a", "b", "c"], "")    = "abc"
     * StringUtils.join([null, "", "a"], ',')   = ",,a"
     * </pre>
     *
     * @param array  the array of values to join together, may be null
     * @param separator  the separator character to use, null treated as ""
     * @param startIndex the first index to start joining from.  It is
     * an error to pass in an end index past the end of the array
     * @param endIndex the index to stop joining from (exclusive). It is
     * an error to pass in an end index past the end of the array
     * @return the joined String, <code>null</code> if null array input
     */
    public static String join(Object[] array, String separator, int startIndex,
          int endIndex) {
        if (array == null) {
            return null;
        }
        if (separator == null) {
            separator = "";
        }

        // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
        //           (Assuming that all Strings are roughly equally long)
        int bufSize = (endIndex - startIndex);
        if (bufSize <= 0) {
            return "";
        }

        bufSize *= ((array[startIndex] == null ? 16
              : array[startIndex].toString().length())
              + separator.length());

        StringBuffer buf = new StringBuffer(bufSize);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                buf.append(array[i]);
            }
        }
        return buf.toString();
    }

    /**
     * <p>Joins the elements of the provided array into a single String
     * containing the provided list of elements.</p>
     *
     * <p>No delimiter is added before or after the list.
     * A <code>null</code> separator is the same as an empty String ("").
     * Null objects or empty strings within the array are represented by
     * empty strings.</p>
     *
     * <pre>
     * StringUtils.join(null, *)                = null
     * StringUtils.join([], *)                  = ""
     * StringUtils.join([null], *)              = ""
     * StringUtils.join(["a", "b", "c"], "--")  = "a--b--c"
     * StringUtils.join(["a", "b", "c"], null)  = "abc"
     * StringUtils.join(["a", "b", "c"], "")    = "abc"
     * StringUtils.join([null, "", "a"], ',')   = ",,a"
     * </pre>
     *
     * @param array  the array of values to join together, may be null
     * @param separator  the separator character to use, null treated as ""
     * @param startIndex the first index to start joining from.  It is
     * an error to pass in an end index past the end of the array
     * @param endIndex the index to stop joining from (exclusive). It is
     * an error to pass in an end index past the end of the array
     * @param internalSep separator for each element in array. each element
     * will be split with the separator and the first element will be
     * added to the buffer
     * @return the joined String, <code>null</code> if null array input
     */
    public static String join(Object[] array, String separator, int startIndex,
          int endIndex, String internalSep) {
        if (array == null) {
            return null;
        }
        if (separator == null) {
            separator = "";
        }

        // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
        //           (Assuming that all Strings are roughly equally long)
        int bufSize = (endIndex - startIndex);
        if (bufSize <= 0) {
            return "";
        }

        bufSize *= ((array[startIndex] == null ? 16
              : array[startIndex].toString().length())
              + separator.length());

        StringBuffer buf = new StringBuffer(bufSize);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                String token = array[i].toString().split(internalSep)[0];
                buf.append(token);
            }
        }
        return buf.toString();
    }
}
