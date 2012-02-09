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
package opennlp.textgrounder.text.prep;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.List;

import opennlp.textgrounder.util.Span;

public class JythonNER extends ScriptNER {
  public JythonNER(String name, NamedEntityType type) {
    super("python", name, type);
  }

  public JythonNER(String name) {
    this(name, NamedEntityType.LOCATION);
  }

  public List<Span<NamedEntityType>> recognize(List<String> tokens) {
    ScriptEngine engine = this.getEngine();
    engine.put("tokens", tokens);

    try {
      engine.eval("spans = recognize(tokens)");
    } catch (ScriptException e) {
      return null;
    }

    List<List<Integer>> tuples = (List<List<Integer>>) engine.get("spans");
    List<Span<NamedEntityType>> spans =
      new ArrayList<Span<NamedEntityType>>(tuples.size());

    for (List<Integer> tuple : tuples) {
      spans.add(new Span<NamedEntityType>(tuple.get(0), tuple.get(1), this.getType()));
    }

    return spans;
  }
}

