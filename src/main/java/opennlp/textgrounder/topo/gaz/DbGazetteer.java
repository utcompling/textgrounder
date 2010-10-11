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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.List;
import java.util.ArrayList;

import opennlp.textgrounder.topo.Coordinate;
import opennlp.textgrounder.topo.Location;
import opennlp.textgrounder.topo.PointRegion;
import opennlp.textgrounder.topo.Region;

public class DbGazetteer extends Gazetteer {
  protected final Connection connection;
  protected PreparedStatement insertStatement;
  protected PreparedStatement lookupStatement;
  protected int inBatch;
  protected final int batchSize;

  public static Gazetteer create(String url) {
    Gazetteer result = null;
    try {
      result = new DbGazetteer(DriverManager.getConnection(url));
    } catch (SQLException e) {
      System.err.format("Could not create database connection: %s\n", e);
      e.printStackTrace();
      System.exit(1);
    }
    return result;
  }

  public DbGazetteer(Connection connection) {
    this(connection, 1024 * 512);
  }

  public DbGazetteer(Connection connection, int batchSize) {
    this.inBatch = 0;
    this.batchSize = batchSize;
    this.connection = connection;
  }

  public void add(String name, Location location) {
    Coordinate coordinate = location.getRegion().getCenter();
    try {
      PreparedStatement statement = this.getInsertStatement();
      statement.setInt(1, location.getId());
      statement.setString(2, name);
      statement.setInt(3, location.getType().ordinal());
      statement.setDouble(4, coordinate.getLat());
      statement.setDouble(5, coordinate.getLng());
      statement.setInt(6, location.getPopulation());
      //statement.setString(7, location.getContainer());
      statement.addBatch();

      if (++this.inBatch == this.batchSize) {        
        this.inBatch = 0;
        statement.executeBatch();
        statement.close();
        /* Ideally we'd just close the statement, but the SQLite driver has a
           problem with isClosed(), so we're setting it to null. */
        this.insertStatement = null;
      }

    } catch (SQLException e) {
      System.err.format("Error while adding location to database: %s\n", e);
      e.printStackTrace();
      System.exit(1); 
    }
  }

  @Override
  public void finishLoading() {
    if (this.inBatch > 0) {
      this.inBatch = 0;
      try {
        this.insertStatement.executeBatch();
        this.insertStatement.close();
        this.insertStatement = null;
      } catch (SQLException e) {
        System.err.format("Error while adding location to database: %s\n", e);
        e.printStackTrace();
        System.exit(1); 
      }
    }
  }

  public List<Location> lookup(String query) {
    ArrayList<Location> locations = new ArrayList<Location>();

    try {
      PreparedStatement statement = this.getLookupStatement();
      statement.setString(1, query);
      ResultSet result = statement.executeQuery();
      while (result.next()) {
        int id = result.getInt(1);
        String name = result.getString(2);
        Location.Type type = Location.Type.values()[result.getInt(3)];
        double lat = result.getDouble(4);
        double lng = result.getDouble(5);
        Coordinate coordinate = Coordinate.fromRadians(lat, lng);
        Region region = new PointRegion(coordinate);
        int population = result.getInt(6);
        locations.add(new Location(id, name, region, type, population));
      }
      result.close();
      locations.trimToSize();
    } catch (SQLException e) {
      System.err.format("Error: could not perform database lookup: %s\n", e);
      e.printStackTrace();
    }

    return locations;
  }

  @Override
  public void close() {
    try {
      this.connection.close();
    } catch (SQLException e) {
      System.err.format("Could not close database connection: %s\n", e);
      e.printStackTrace();
    }
  }

  protected void createSchema() throws SQLException {
    StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE places (");
    builder.append("  id INTEGER PRIMARY KEY,");
    builder.append("  name VARCHAR(256),");
    builder.append("  type VARCHAR(256),");
    builder.append("  lat DOUBLE,");
    builder.append("  lng DOUBLE,");
    builder.append("  pop INTEGER,");
    builder.append("  container VARCHAR(256))");

    Statement statement = this.connection.createStatement();
    statement.executeUpdate(builder.toString());
    statement.executeUpdate("CREATE INDEX nameIdx ON places (name)");
    statement.close();
  }

  protected PreparedStatement getInsertStatement() throws SQLException {
    if (this.insertStatement == null) {
      String query = "INSERT INTO places VALUES (?, ?, ?, ?, ?, ?, ?)";
      this.insertStatement = this.connection.prepareStatement(query);
    }
    return this.insertStatement;
  }

  protected PreparedStatement getLookupStatement() throws SQLException {
    if (this.lookupStatement == null) {
      String query = "SELECT * FROM places WHERE name = ?";
      this.lookupStatement = this.connection.prepareStatement(query);
    }
    return this.lookupStatement;
  }
}

