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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public abstract class ScriptNER implements NamedEntityRecognizer {
  private final String language;
  private final String name;
  private final NamedEntityType type;
  private final ScriptEngine engine;

  /** 
    * Constructor for classes that use the JSR-223 scripting engine to perform
    * named entity recognition.
    *
    * @param language The JSR-223 name of the scripting language
    * @param name     The path to the resource containing the script
    * @param type     The kind of named entity that is recognized
    */
  public ScriptNER(String language, String name, NamedEntityType type) {
    this.language = language;
    this.name = name;
    this.type = type;

    ScriptEngineManager manager = new ScriptEngineManager();
    this.engine = manager.getEngineByName(this.language);

    try {
      InputStream stream = ScriptNER.class.getResourceAsStream(this.name);
      InputStreamReader reader = new InputStreamReader(stream);
      this.engine.eval(reader);
      stream.close();
    } catch (ScriptException e) {
      System.err.println(e);
      System.exit(1);
    } catch (IOException e) {
      System.err.println(e);
      System.exit(1);
    }
  }

  public ScriptNER(String language, String name) {
    this(language, name, NamedEntityType.LOCATION);
  }

  protected ScriptEngine getEngine() {
    return this.engine;
  }

  protected NamedEntityType getType() {
    return this.type;
  }
}

