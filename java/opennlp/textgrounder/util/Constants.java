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

import java.text.DecimalFormat;


/**
 * Class for keeping constant values.
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.53 $, $Date: 2006/10/12 21:20:44 $
 */
public class Constants {

    // the location of TextGrounder
    public final static String TEXTGROUNDER_HOME = System.getProperty("textgrounder.home");

    // the location of the user's home directory
    public final static String USER_HOME = System.getProperty("user.home");

    // the current working directory
    public final static String CWD = System.getProperty("user.dir");

    // The format for printing precision, recall and f-scores.
    public static final DecimalFormat PERCENT_FORMAT = 
	new DecimalFormat("#,##0.00%");


}
