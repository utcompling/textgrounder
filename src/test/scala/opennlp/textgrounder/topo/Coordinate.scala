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

  "A degree-constructed coordinate" should {
    val coordinate = Coordinate.fromDegrees(45, -45)
    "have the correct radian value for latitude" in {
      coordinate.getLat must_== math.Pi / 4
    }

    "have the correct radian value for longitude" in {
      coordinate.getLng must_== -math.Pi / 4
    }

    "be equal to its radian-constructed equivalent" in {
      coordinate must_== Coordinate.fromRadians(math.Pi / 4, -math.Pi / 4)
    }
  }

  "A coordinate at the origin" should {
    val coordinate = Coordinate.fromDegrees(0, 0)
    "have the correct angular distance from a coordinate 1 radian away horizontally" in {
      coordinate.distance(Coordinate.fromRadians(0, 1)) must_== 1
    }

    "have the correct distance from a coordinate 1 radian away vertically" in {
      coordinate.distance(Coordinate.fromRadians(1, 0)) must_== 1
    }
  }
}

