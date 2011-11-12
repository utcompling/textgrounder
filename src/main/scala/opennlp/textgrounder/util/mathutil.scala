///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.util

package object mathutil {
  /**
   *  Return the median value of a list.  List will be sorted, so this is O(n).
   */
  def median(list: Seq[Double]) = {
    val sorted = list.sorted
    val len = sorted.length
    if (len % 2 == 1)
      sorted(len / 2)
    else {
      val midp = len / 2
      0.5*(sorted(midp-1) + sorted(midp))
    }
  }
  
  /**
   *  Return the mean of a list.
   */
  def mean(list: Seq[Double]) = {
    list.sum / list.length
  }
} 
