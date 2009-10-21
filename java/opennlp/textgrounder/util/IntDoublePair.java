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
