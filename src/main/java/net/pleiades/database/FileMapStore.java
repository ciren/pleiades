/* Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.database;

import com.hazelcast.core.MapStore;
import com.hazelcast.util.ConcurrentHashSet;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.pleiades.persistence.PersistentFileMapObject;

/**
 *
 * @author bennie
 */
public class FileMapStore implements MapStore<String, byte[]> {
    private static final String configFile = "pleiades.conf"; //fix this if you can
    private DBCollection files;

    public FileMapStore() {
        if (!connect()) {
            System.out.println(">ERROR: Unable to connect to persistent store. Contact administrator.");
            System.exit(1);
        }
        System.out.println(">[Connected to files map store]");
    }
    
    private boolean connect() {
        Properties properties = loadConfiguration();
        Mongo mongo;
        String storeAddress = properties.getProperty("persistent_store_address");
        int storePort = Integer.valueOf(properties.getProperty("persistent_store_port"));
        String pass = properties.getProperty("persistent_store_password");
        String user = properties.getProperty("persistent_store_user");
        boolean auth = false;
        
        try {
            mongo = new Mongo(storeAddress, storePort);
            mongo.setWriteConcern(WriteConcern.SAFE);
            DB db = mongo.getDB("Pleiades");
            auth = db.authenticate(user, pass.toCharArray());
            files = db.getCollection(properties.getProperty("file_map"));
            files.setObjectClass(PersistentFileMapObject.class);
        } catch (Exception e) {
            return false;
        }
        
        return auth;
    }
    
    @Override
    public void store(String k, byte[] v) {
        DBObject o = new PersistentFileMapObject(k, v);
        
        BasicDBObject query = new BasicDBObject();
        query.put("file_key", k);
        
        if (files.find(query).toArray().isEmpty()) {
            files.insert(o);
        } else {
            files.findAndModify(query, o);
        }
    }

    @Override
    public void storeAll(Map<String, byte[]> map) {
        for (String k : map.keySet()) {
            store(k, map.get(k));
        }
    }

    @Override
    public void delete(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("file_key", k);
        
        files.remove(query);
    }

    @Override
    public void deleteAll(Collection<String> clctn) {
        for (String k : clctn) {
            delete(k);
        }
    }

    @Override
    public byte[] load(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("file_key", k);
        
        DBObject load = files.findOne(query);
        
        if (load == null) {
            return null;
        }
        
        return ((PersistentFileMapObject)load).file();
    }

    @Override
    public Map<String, byte[]> loadAll(Collection<String> clctn) {
        Map<String, byte[]> file_map = new ConcurrentHashMap<String, byte[]>();
        
        for (String k : clctn) {
            file_map.put(k, load(k));
        }
        
        return file_map;
    }

    @Override
    public Set<String> loadAllKeys() {
        Set<String> keys = new ConcurrentHashSet<String>();
        BasicDBObject query = new BasicDBObject();
        
        DBCursor cursor = files.find(query);
        
        while (cursor.hasNext()) {
            keys.add((String) (cursor.next()).get("file_key"));
        }
        
        return keys;
    }
    
    private static Properties loadConfiguration() {
        Properties p = new Properties();
        
        try {
            p.load(new FileInputStream(configFile));
        } catch (IOException e) {
            throw new Error(">ERROR: Unable to load configuration file " + configFile);
        }
        
        return p;
    }
}
