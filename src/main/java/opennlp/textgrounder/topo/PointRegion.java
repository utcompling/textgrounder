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
package opennlp.textgrounder.topo;

import java.util.ArrayList;
import java.util.List;

public class PointRegion extends Region {

  private static final long serialVersionUID = 42L;

  private Coordinate coordinate;

  public PointRegion(Coordinate coordinate) {
    this.coordinate = coordinate;
  }

  public Coordinate getCenter() {
    return this.coordinate;
  }

    public void setCenter(Coordinate coord) {
        this.coordinate = coord;
    }

  public boolean contains(double lat, double lng) {
    return lat == this.coordinate.getLat() && lng == this.coordinate.getLng();
  }

  public double getMinLat() {
    return this.coordinate.getLat();
  }

  public double getMaxLat() {
    return this.coordinate.getLat();
  }

  public double getMinLng() {
    return this.coordinate.getLng();
  }

  public double getMaxLng() {
    return this.coordinate.getLng();
  }

  public List<Coordinate> getRepresentatives() {
    List<Coordinate> representatives = new ArrayList<Coordinate>(1);
    representatives.add(this.coordinate);
    return representatives;
  }

    public void setRepresentatives(List<Coordinate> coordinates) {
        this.coordinate = coordinates.get(0);
    }
}

