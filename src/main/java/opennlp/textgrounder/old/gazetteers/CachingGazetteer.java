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
package opennlp.textgrounder.old.gazetteers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.textgrounder.old.topostructs.Location;

public class CachingGazetteer extends Gazetteer {
  protected final Gazetteer source;
  protected final boolean cacheIncoming;
  protected final Map<String, List<Location>> cache;

  public CachingGazetteer(Gazetteer source) {
    this(source, false);
  }

  public CachingGazetteer(Gazetteer source, boolean cacheIncoming) {
    this.source = source;
    this.cacheIncoming = cacheIncoming;
    this.cache = this.initialize();
  }

  protected Map<String, List<Location>> initialize() {
    return new HashMap<String, List<Location>>();
  }

  public void add(String name, Location location) {
    if (this.cacheIncoming) {
      List<Location> result = this.lookup(name);
      if (result == null) {
        result = new ArrayList<Location>();
      }
      result.add(location);
      this.cache.put(name, result);
    }
    this.source.add(name, location);
  }

  @Override
  public void finishLoading() {
    this.source.finishLoading();
  }

  public List<Location> lookup(String query) {
    List<Location> result = this.cache.get(query);
    if (result == null) {
      result = this.source.lookup(query);
      this.cache.put(query, result);
    }
    return result;
  }

  public void close() {
    this.source.close();
  }
}

