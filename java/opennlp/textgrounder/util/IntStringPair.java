package opennlp.textgrounder.util;

import gnu.trove.*;

/**
 * A pair of an int and String
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.1 $, $Date: 2005/04/14 04:22:57 $
 */
public final class IntStringPair extends TLinkableAdapter implements Comparable<IntStringPair> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
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
