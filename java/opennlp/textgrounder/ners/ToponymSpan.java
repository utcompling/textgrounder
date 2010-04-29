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
package opennlp.textgrounder.ners;

/**
 * Class for handling toponyms in a natural language text. Pair object that
 * contains a beginning index and an ending index for a given toponym. The indexes
 * are given in terms of words. At the moment, toponyms are identified by
 * the Stanford NER classifier and ToponymSpan are populated and tabulated in
 * {@link SNERPairListSet#addToponymSpansFromFile(java.lang.String)}.
 *
 * @author
 */
public class ToponymSpan {

    public int begin;
    public int end;

    public ToponymSpan(int b, int e) {
        begin = b;
        end = e;
    }

    public int getLength() {
        return end - begin;
    }
}
