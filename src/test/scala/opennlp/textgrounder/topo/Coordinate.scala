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
package opennlp.textgrounder.topo

import org.specs._
import org.specs.runner._

class CoordinateTest extends JUnit4(CoordinateSpec)
object CoordinateSpec extends Specification {

  "Coordinate.fromDegrees(45.0, -45.0)" should {
    val coordinate = Coordinate.fromDegrees(45.0, -45.0)
    "have the correct value for latitude" in {
      coordinate.getLat must_== math.Pi / 4
    }

    "have the correct value for longitude" in {
      coordinate.getLng must_== -math.Pi / 4
    }
  }
}

