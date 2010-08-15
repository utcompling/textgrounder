///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.bayesian.structs;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class IntDoublePair implements Comparable<IntDoublePair> {

    public int index = 0;
    public double count = 0;

    public IntDoublePair(int _wordid, double _count) {
        index = _wordid;
        count = _count;
    }

    /**
     * sorting order is reversed -- higher (double) values come first
     *
     * @param p
     * @return
     */
    @Override
    public int compareTo(IntDoublePair p) {
        if (count < p.count) {
            return 1;
        } else if (count > p.count) {
            return -1;
        } else {
            return 0;
        }

    }
}
