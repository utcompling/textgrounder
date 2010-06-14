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

import java.text.DecimalFormat;


/**
 * Class for keeping constant values.
 *
 * @author  Jason Baldridge
 * @version $Revision: 1.53 $, $Date: 2006/10/12 21:20:44 $
 */
public class Constants {

    /**
     * Machine epsilon for comparing equality in floating point numbers.
     */
    public static final double EPSILON = 1e-6;

    // the location of TextGrounder
    public final static String TEXTGROUNDER_DIR = System.getenv("TEXTGROUNDER_DIR");
    public final static String TEXTGROUNDER_DATA = System.getenv("TEXTGROUNDER_DATA");

    // the location of the Stanford NER system
    public final static String STANFORD_NER_HOME = System.getenv("STANFORD_NER_HOME");

    // the location of the World Gazetteer database file
//    public final static String WGDB_PATH = System.getenv("WGDB_PATH");

    // the location of the TR-CoNLL database file
//    public static final String TRDB_PATH = System.getenv("TRDB_PATH");

    // the location of the user's home directory
    public final static String USER_HOME = System.getProperty("user.home");

    // the current working directory
    public final static String CWD = System.getProperty("user.dir");

    // The format for printing precision, recall and f-scores.
    public static final DecimalFormat PERCENT_FORMAT = 
	new DecimalFormat("#,##0.00%");


}
