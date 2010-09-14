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
package opennlp.textgrounder.gazetteers;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import opennlp.textgrounder.topostructs.Coordinate;
import opennlp.textgrounder.topostructs.Location;

public class TempDbGazetteer extends DbGazetteer {
  private final File resource;

  protected TempDbGazetteer(Connection connection, File resource) {
    super(connection);
    this.resource = resource;
    this.resource.deleteOnExit();

    try {
      this.createSchema();
    } catch (SQLException e) {
      System.err.format("Error creating temporary database: %s\n", e);
      e.printStackTrace();
      System.exit(1); 
    }
  }

  public static TempDbGazetteer createDerby() {
    return TempDbGazetteer.createTemporary("jdbc:derby:%s;create=true");
  }

  public static TempDbGazetteer createH2() {
    return TempDbGazetteer.createTemporary("jdbc:h2:%s", "%s.h2.db");
  }

  public static TempDbGazetteer createSQLite() {
    return TempDbGazetteer.createTemporary("jdbc:sqlite:/%s");
  }

  public String getPath() {
    return this.resource.getPath();
  }

  protected static String createTempPath() {
    return String.format("%s%stg-%d.gaz", System.getProperty("java.io.tmpdir"), File.separator, System.nanoTime());
  }

  protected static TempDbGazetteer createTemporary(String urlFormat) {
    return TempDbGazetteer.createTemporary(urlFormat, "%s");
  }

  protected static TempDbGazetteer createTemporary(String urlFormat, String fullPathFormat) {
    TempDbGazetteer result = null;
    String path = TempDbGazetteer.createTempPath();
    String fullPath = String.format(fullPathFormat, path);
    String url = String.format(urlFormat, path);

    try {
      Connection connection = DriverManager.getConnection(url);
      result =  new TempDbGazetteer(connection, new File(fullPath));
    } catch (SQLException e) {
      System.err.format("Could not create database connection: %s\n", e);
      e.printStackTrace();
      System.exit(1);
    }

    return result;
  }

  public void close() {
    super.close();
    if (this.resource.isDirectory()) {
      for (File file : this.resource.listFiles()) {
        System.out.format("Deleting: %s\n", file);
        //file.delete();
      }
    }
  }
}

