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
package opennlp.textgrounder.annealers;

import opennlp.textgrounder.geo.CommandLineOptions;

/**
 * Maximum posterior decoder. Simply returns the largest value among the arrays.
 * 
 * @author tsmoon
 */
public class MaximumPosteriorDecoder extends Annealer {

    private int count = 1;
    
    public MaximumPosteriorDecoder() {
    }

    public MaximumPosteriorDecoder(CommandLineOptions options) {
        super(options);
    }

    @Override
    public double annealProbs(int starti, double[] classes) {
        double max = 0;
        int maxid = 0;
        try {
            for (int i = starti;; ++i) {
                if (classes[i] > max) {
                    max = classes[i];
                    maxid = i;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        try {
            for (int i = starti;; ++i) {
                classes[i] = 0;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
        }
        classes[maxid] = 1;
        return 1;
    }

    @Override
    public boolean nextIter() {
        if(count != 0) {
            count = 0;
            return true;
        } else {
            return false;
        }
    }
}
