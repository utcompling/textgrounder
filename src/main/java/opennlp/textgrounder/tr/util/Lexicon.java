///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.tr.util;

import java.io.Serializable;
import java.util.List;

public interface Lexicon<A extends Serializable>
  extends Serializable, Iterable<A> {
  public boolean contains(A entry);
  public int get(A entry);
  public int getOrAdd(A entry);
  public A atIndex(int index);
  public int size();
  public boolean isGrowing();
  public void stopGrowing();
  public void startGrowing();
  public List<Integer> concatenate(Lexicon<A> other);
}

