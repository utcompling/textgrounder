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
import edu.stanford.nlp.ie.crf.CRFClassifier;

import java.util.Hashtable;
import java.util.List;

import opennlp.textgrounder.gazetteers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.topostructs.*;

/**
 *
 * @author tsmoon
 */
public abstract class Model {

    private int barScale = 50000;
    private Gazetteer gazetteer;
    private Hashtable<String, List<Location>> gazCache = new Hashtable<String, List<Location>>();
    private double degreesPerRegion = 3.0;
    private Region[][] regionArray;
    private int regionArrayWidth, regionArrayHeight, activeRegions;
    private CRFClassifier classifier;
    private DocumentSet docSet;
    private boolean initializedXMLFile = false;
    private boolean finalizedXMLFile = false;

    protected Model() {
    }

    protected abstract void allocateFields(DocumentSet docSet, int T);

    protected abstract void initializeFromOptions(CommandLineOptions options);
}
