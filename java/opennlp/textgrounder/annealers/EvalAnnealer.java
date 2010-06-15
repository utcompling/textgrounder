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
package opennlp.textgrounder.annealers;

/**
 * Class for conducting simulated annealing. This class also controls the burn-in
 * and the number of samples taken.
 * 
 * @author tsmoon
 */
public class EvalAnnealer extends EmptyAnnealer {

    public EvalAnnealer() {
        innerIterationsMax = 100;
        outerIterationsMax = 1;
    }

    public EvalAnnealer(int _innerIterationsMax) {
        innerIterationsMax = _innerIterationsMax;
        outerIterationsMax = 1;
    }
}
