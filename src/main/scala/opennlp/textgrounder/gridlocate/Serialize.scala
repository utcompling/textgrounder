///////////////////////////////////////////////////////////////////////////////
//  Serialize.scala.notyet
//
//  Copyright (C) 2013 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package gridlocate

import java.io._
import com.nicta.scoobi.core.WireFormat
import WireFormat._

import util.error.RethrowableRuntimeException
import learning.LabelIndex

/**
 * An exception thrown to indicate that an error occurred during
 * serialization or deserialization.
 */
case class SerializationError(
  message: String,
  cause: Option[Throwable] = None
) extends RethrowableRuntimeException(message, cause)

class GridRankerDataSerialize[Co : WireFormat](
  grid: Grid[Co],
  docs: Iterable[GridDoc[Co]]
) {
  implicit val wfGridCell = new GridCellWireFormat(grid)
  val docmap = docs.map { doc => doc.title -> doc }.toMap
  implicit val wfGridDoc = new GridDocWireFormat(docmap)

  def write_training_data(file: String,
      grid: Grid[Co], docs: Iterable[GridDoc[Co]],
      data: IndexedSeq[(GridRankerInst[Co], LabelIndex)]) {
    val outstream = new DataOutputStream(
      new BufferedOutputStream(
        new FileOutputStream(file)))
  }
}
