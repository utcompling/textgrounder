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
package opennlp.textgrounder.models;

import opennlp.textgrounder.io.DocumentSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.ners.*;

/**
 * Topic model with region awareness. Toponyms are all unigrams. Multiword
 * toponyms are split into space delimited tokens.
 * 
 * @author tsmoon
 */
public class NgramRegionModel extends UnigramRegionModel {

    public NgramRegionModel(CommandLineOptions options) {
        super(options);
        regionMap = new Hashtable<Integer, Region>();
        reverseRegionMap = new Hashtable<Region, Integer>();
        nameToRegionIndex = new Hashtable<String, HashSet<Integer>>();

        T = 0;
        setAllocateRegions();
    }

    /**
     * Allocate memory for fields
     *
     * @param docSet Container holding training data.
     * @param T Number of regions
     */
    @Override
    protected void allocateFields(DocumentSet docSet, int T) {
        N = 0;
        W = docSet.wordsToInts.size();
        D = docSet.size();
        betaW = beta * W;
        for (int i = 0; i < docSet.size(); i++) {
            N += docSet.get(i).size();
        }

        documentVector = new int[N];
        wordVector = new int[N];
        topicVector = new int[N];
        toponymVector = new int[N];

        for (int i = 0; i < N; ++i) {
            toponymVector[i] = documentVector[i] = wordVector[i] = topicVector[i] = 0;
        }

        int tcount = 0, docs = 0;
        for (ArrayList<Integer> doc : docSet) {
            int offset = 0;
            for (int idx : doc) {
                wordVector[tcount + offset] = idx;
                documentVector[tcount + offset] = docs;
                offset += 1;
            }
            tcount += doc.size();
            docs += 1;
        }

    }
}
