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
package opennlp.textgrounder.topo.gaz;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

import opennlp.textgrounder.topo.Location;

public class SoftCachingGazetteer extends CachingGazetteer {
  public SoftCachingGazetteer(Gazetteer source) {
    this(source, true);
  }

  public SoftCachingGazetteer(Gazetteer source, boolean cacheIncoming) {
    super(source, cacheIncoming);
  }

  @Override
  protected Map<String, List<Location>> initialize() {
    return new MapMaker().softValues().makeComputingMap(
      new Function<String, List<Location>>() {
        public List<Location> apply(String query) {
          return SoftCachingGazetteer.this.lookup(query);
        }
      }
    );
  }

  @Override
  public List<Location> lookup(String query) {
    query = query.toLowerCase();
    return this.cache.get(query);
  }
}

