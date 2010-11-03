///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
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
package opennlp.textgrounder.old.gazetteers.older;

import gnu.trove.*;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.*;

import opennlp.textgrounder.old.textstructs.older.Lexicon;
import opennlp.textgrounder.old.topostructs.*;

/**
 * Base class for Gazetteer objects. Creates gazetteer databases (i.e. db's that
 * contain names of places around the world and geographic information),
 * extracts information from them, and dumps them into internally accessible
 * collections.
 * 
 * The Gazetteer class itself is essentially a mapping from toponyms (words that
 * name a place, e.g. "London") to a set of locations (stored in Location
 * objects) that have the toponym as their name. Both toponyms and locations are
 * identified by unique ID's (integers), with the mappings from ID's to toponyms
 * or locations stored separately. (`toponymLexicon' stores the mapping from
 * ID's to toponym strings, and `idxToLocationMap' stores the mapping from ID's
 * to Location objects.) Thus, the implementation of the Gazetteer is actually a
 * hashmap from integer keys (toponyms) to hashsets of integers (sets of
 * locations).
 * 
 * During creation time (specifically, in the initialize() funtion), the
 * gazetteer data is read from a source file, which contains geographic
 * information in some format that is specific to the particular file. The data
 * is then dumped to a temporary database file (`.db' extension, SQLite format).
 * 
 * Note, however, that the Gazetteer object isn't actually populated from the
 * database until a corpus is read in, and it's populated only with toponyms
 * that appear in the corpus, mapped to all possible locations for the toponym.
 * 
 * The various subtypes of this class handle data in different formats, from
 * gazetteers obtained from various sources.
 */
public abstract class Gazetteer extends TIntObjectHashMap<TIntHashSet> {

    /*public final static int USGS_TYPE = 0;
    public final static int US_CENSUS_TYPE = 1;
    
    public final static int DEFAULT_TYPE = USGS_TYPE;*/
    /**
     * Cache that maps location ID's to Location objects. Only holds locations
     * that we've already encountered in the corpus. Location ID's are used as
     * hash keys; see above.
     */
    protected TIntObjectHashMap<Location> idxToLocationMap;
    /**
     * Flag for refreshing gazetteer from original database
     */
    public boolean gazetteerRefresh = false;
    /**
     * Lookup table mapping toponym ID's to toponym strings. Only holds toponyms
     * we've already encountered in the corpus. Toponym ID's are used as hash
     * keys; see above.
     */
    protected Lexicon toponymLexicon;
    protected Connection conn;
    protected Statement stat;
    protected static Pattern allDigits = Pattern.compile("^[0-9]+$");
    protected static Pattern rawCoord = Pattern.compile("^\\-?[0-9]+$");
    /**
     * The largest location id value that has been activated. Needed for
     * creating pseudo locations later in region model
     */
    protected static int maxLocId = 0;

    protected Gazetteer() {
    }

    /**
     * Default constructor. Allocates memory for internal collections.
     * 
     * @param location
     *            empty parameter only to be overridden in derived classes
     */
    public Gazetteer(String location) {
        idxToLocationMap = new TIntObjectHashMap<Location>();
        toponymLexicon = new Lexicon();
    }

    /**
     * Abstract initialization function. Generally includes db building and
     * reading, and population of collections and the gazetteer.
     * 
     * @param location
     * @throws FileNotFoundException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    abstract void initialize(String location) throws FileNotFoundException,
          IOException, ClassNotFoundException, SQLException;

    /**
     * Search for a toponym in the Gazetteer object and return the set of
     * locations associated with it. If the toponym hasn't been seen, look in
     * the underlying database to find the set of locations associated with the
     * toponym and add a new toponym->locations mapping in the Gazetteer.
     * 
     * @param placename
     *            the placename to lookup in the gazetteer
     * @return the indexes of the locations that are associated with the current
     *         placename
     * @throws SQLException
     */
    public TIntHashSet get(String placename) {
        int topid = toponymLexicon.addWord(placename);
        return get(placename, topid);
    }

    /**
     * Same as get(placename) but is also passed in the integer ID of the
     * placename. Actually implements the logic of get(placename).
     * 
     * FIXME: This should actually be calling frugalGet() rather than
     * duplicating the code to do the database lookup.
     * 
     * @param placename
     * @param topid
     * @return
     */
    public TIntHashSet get(String placename, int topid) {
        placename = placename.toLowerCase();
        try {
            TIntHashSet locationsToReturn = get(topid);
            if (locationsToReturn == null) {
                locationsToReturn = new TIntHashSet();
                ResultSet rs = stat.executeQuery("select * from places where name = \""
                      + placename + "\";");
                while (rs.next()) {
                    Location locationToAdd = new Location(rs.getInt("id"),
                          rs.getString("name"),
                          rs.getString("type"),
                          new Coordinate(rs.getDouble("lon"), rs.getDouble("lat")),
                          rs.getInt("pop"),
                          rs.getString("container"),
                          0);
                    // System.out.println("Location: " + locationToAdd);
                    idxToLocationMap.put(locationToAdd.getId(), locationToAdd);
                    locationsToReturn.add(locationToAdd.getId());
                    if (locationToAdd.getId() > maxLocId) {
                        maxLocId = locationToAdd.getId();
                    }
                }
                rs.close();
                put(topid, locationsToReturn);
            }

            return locationsToReturn;
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    /**
     * Return a set of all locations in the underlying gazetteer database.
     * FIXME: This should probably throw an error rather than providing a wrong
     * default implementation.
     * 
     * @return
     * @throws Exception
     */
    public TIntHashSet getAllLocalities() throws Exception { // overridden by
        // WGGazetteer only
        // right now
        return new TIntHashSet();
    }

    /**
     * Return the cache mapping location ID's to Location objects.
     * 
     * @return the idxToLocationMap
     */
    public TIntObjectHashMap<Location> getIdxToLocationMap() {
        return idxToLocationMap;
    }

    /**
     * Return the Location object corresponding to a location ID, for ID's that
     * have been cached in `idxToLocationMap'.
     * 
     * @param locid
     * @return
     */
    public Location getLocation(int locid) {
        return idxToLocationMap.get(locid);
    }

    /**
     * Add location to idxToLocationMap, using a newly generated location ID.
     * Only for pseudo-locations that must be resolved after training on corpus
     * is complete. These locations do not actually have any corresponding
     * points in the gazetteer. (Locations in the gazetteer come with a
     * predefined ID.)
     * 
     * @param loc
     * @return
     */
    public int putLocation(Location loc) {
        idxToLocationMap.put(loc.getId(), loc);
        return loc.getId();
    }

    /**
     * Return the cache that maps location ID's to Location objects.
     * 
     * @return the toponymLexicon
     */
    public Lexicon getToponymLexicon() {
        return toponymLexicon;
    }

    /**
     * Return whether a given toponym exists in the `toponymLexicon' cache.
     * 
     * @param placename
     * @return
     */
    public boolean contains(String placename) {
        return toponymLexicon.contains(placename);
    }

    /**
     * @return the maxLocId
     */
    public int getMaxLocId() {
        return maxLocId;
    }
}
