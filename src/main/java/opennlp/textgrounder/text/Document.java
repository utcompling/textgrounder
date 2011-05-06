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
package opennlp.textgrounder.text;

import java.io.*;
import opennlp.textgrounder.topo.*;

public abstract class Document<A extends Token> implements Iterable<Sentence<A>>, Serializable {
  protected final String id;
  protected Coordinate goldCoord;
  protected Coordinate systemCoord;
  protected String timestamp;

  public Document(String id) {
      this(id, null, null, null);
  }

  public Document(String id, String timestamp, Coordinate goldCoord) {
      this(id, timestamp, goldCoord, null);
  }

  public Document(String id, String timestamp, Coordinate goldCoord, Coordinate systemCoord) {
    this.id = id;
    this.timestamp = timestamp;
    this.goldCoord = goldCoord;
    this.systemCoord = systemCoord;
  }

  public String getId() {
    return this.id;
  }

    public Coordinate getGoldCoord() {
        return this.goldCoord;
    }

    public Coordinate getSystemCoord() {
        return this.systemCoord;
    }

    public void setSystemCoord(Coordinate systemCoord) {
        this.systemCoord = systemCoord;
    }

    public void setSystemCoord(double systemLat, double systemLon) {
        this.systemCoord = Coordinate.fromDegrees(systemLat, systemLon);
    }

    public String getTimestamp() {
        return this.timestamp;
    }
}

