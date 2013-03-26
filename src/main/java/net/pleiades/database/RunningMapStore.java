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
import net.pleiades.persistence.PersistentRunningMapObject;

/**
 *
 * @author bennie
 */
public class RunningMapStore implements MapStore<String, String> {
    private static final String configFile = "pleiades.conf"; //fix this if you can
    private DBCollection running;
    
    public RunningMapStore() {
        if (!connect()) {
            System.out.println("ERROR: Unable to connect to persistent store. Contact administrator.");
            System.exit(1);
        }
        System.out.println("Connected to running store");
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
            running = db.getCollection(properties.getProperty("running_map"));
            running.setObjectClass(PersistentRunningMapObject.class);
        } catch (Exception e) {
            return false;
        }
        
        return auth;
    }
    
    @Override
    public void store(String k, String v) {
        DBObject o = new PersistentRunningMapObject(k, v);
        
        BasicDBObject query = new BasicDBObject();
        query.put("task_id", k);
        
        if (running.find(query).toArray().isEmpty()) {
            running.insert(o);
        } else {
            running.findAndModify(query, o);
        }
    }

    @Override
    public void storeAll(Map<String, String> map) {
        for (String k : map.keySet()) {
            store(k, map.get(k));
        }
    }

    @Override
    public void delete(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("task_id", k);
        
        running.remove(query);
    }

    @Override
    public void deleteAll(Collection<String> clctn) {
        for (String k : clctn) {
            delete(k);
        }
    }

    @Override
    public String load(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("task_id", k);
        
        DBObject load = running.findOne(query);
        
        if (load == null) {
            return null;
        }
        
        return ((PersistentRunningMapObject)load).workerID();
    }

    @Override
    public Map<String, String> loadAll(Collection<String> clctn) {
        Map<String, String> running_map = new ConcurrentHashMap<String, String>();
        
        for (String k : clctn) {
            running_map.put(k, load(k));
        }
        
        return running_map;
    }

    @Override
    public Set<String> loadAllKeys() {
        Set<String> keys = new ConcurrentHashSet<String>();
        BasicDBObject query = new BasicDBObject();
        
        DBCursor cursor = running.find(query);
        
        while (cursor.hasNext()) {
            keys.add((String) cursor.next().get("task_id"));
        }
        
        return keys;
    }
        
    private static Properties loadConfiguration() {
        Properties p = new Properties();
        
        try {
            p.load(new FileInputStream(configFile));
        } catch (IOException e) {
            throw new Error("Unable to load configuration file " + configFile);
        }
        
        return p;
    }
}
