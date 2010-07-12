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
package opennlp.textgrounder.gazetteers;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.*;

import gnu.trove.*;
import opennlp.textgrounder.geo.CommandLineOptions;

import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.*;

/**
 * USGS Gazetteer type.  FIXME: Document me!
 * 
 * @author Taesun Moon
 *
 * @param <E>
 */
public class USGSGazetteer extends Gazetteer {

    public USGSGazetteer(String location) throws
          FileNotFoundException, IOException {
        super(location);
        initialize(Constants.TEXTGROUNDER_DATA + "/gazetteer/pop_places_plaintext.txt.gz");
    }

    @Override
    protected void initialize(String location) throws FileNotFoundException,
          IOException {

        //BufferedReader gazIn = new BufferedReader(new FileReader(location));

        BufferedReader gazIn = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(location))));

        System.out.print("Populating USGS gazetteer from " + location + " ...");

        String curLine;
        String[] tokens;

        curLine = gazIn.readLine(); // first line of gazetteer is legend
        while (true) {
            curLine = gazIn.readLine();
            if (curLine == null) {
                break;
            }
            //System.out.println(curLine);
            tokens = curLine.split("\\|");
            /*for(String token : tokens) {
            System.out.println(token);
            }*/
            if (tokens.length < 17) {
                System.out.println("\nNot enough columns found; this file format should have at least " + 17 + " but only " + tokens.length + " were found. Quitting.");
                System.exit(0);
            }
            Coordinate curCoord =
                  new Coordinate(Double.parseDouble(tokens[9]), Double.parseDouble(tokens[10]));

            int topidx = toponymLexicon.addWord(tokens[1].toLowerCase());
            put(topidx, null);
            topidx = toponymLexicon.addWord(tokens[5].toLowerCase());
            put(topidx, null);
            topidx = toponymLexicon.addWord(tokens[16].toLowerCase());
            put(topidx, null);
        }

        gazIn.close();

        System.out.println("done. Total number of actual place names = " + size());

        /*System.out.println(placenamesToCoords.get("salt springs"));
        System.out.println(placenamesToCoords.get("galveston"));*/

    }

    public TIntHashSet get(String placename) {
        return new TIntHashSet();
    }
}
