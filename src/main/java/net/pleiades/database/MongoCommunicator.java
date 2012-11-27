/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.database;

import com.hazelcast.util.ConcurrentHashSet;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.pleiades.persistence.PersistentCilibSimulation;
import net.pleiades.persistence.PersistentStoreCommunicator;
import net.pleiades.simulations.CilibSimulation;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class MongoCommunicator implements PersistentStoreCommunicator {
    private static Properties properties;
    private DBCollection simulations;

    public MongoCommunicator(Properties p) {
        MongoCommunicator.properties = p;
    }
    
    @Override
    public boolean connect() {
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
            
            simulations = db.getCollection("simulations");
            simulations.setObjectClass(PersistentCilibSimulation.class);
        } catch (Exception e) {
            return false;
        }
        
        return auth;
    }

    @Override
    public void store(DBObject o) {
        BasicDBObject query = new BasicDBObject();

        query.put("id", o.get("id"));
        
        if (simulations.find(query).toArray().isEmpty()) {
            simulations.insert(o);
        } else {
            simulations.findAndModify(query, o);
        }
    }

    @Override
    public Simulation load(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("id", k);
        
        Simulation s = new CilibSimulation((PersistentCilibSimulation) simulations.findOne(query));
        
        return s;
    }

    @Override
    public void storeAll(Map<String, Simulation> map) {
        BasicDBObject query;

        for (String k : map.keySet()) {
            Simulation current = map.get(k);
            PersistentCilibSimulation persist = new PersistentCilibSimulation(current);
            query = new BasicDBObject();
            query.put("id", current.getID());

            if (simulations.find(query).toArray().isEmpty()) {
                simulations.insert(persist);
            } else {
                simulations.findAndModify(query, persist);
            }
        }
    }

    @Override
    public void delete(String k) {
        BasicDBObject query = new BasicDBObject();
        query.put("id", k);
        
        simulations.remove(query);
    }

    @Override
    public void deleteAll(Collection<String> clctn) {
        BasicDBObject query;
        
        for (String k : clctn) {
            query = new BasicDBObject();
            query.put("id", k);
            
            simulations.remove(query);
        }
    }

    @Override
    public Map<String, Simulation> loadAll(Collection<String> clctn) {
        Map<String, Simulation> sims = new ConcurrentHashMap<String, Simulation>();
        BasicDBObject query;
        
        for (String k : clctn) {
            query = new BasicDBObject();
            query.put("id", k);
            
            sims.put(k, new CilibSimulation((PersistentCilibSimulation)simulations.findOne(query)));
        }
        
        return sims;
    }

    @Override
    public Set<String> loadAllKeys() {
        Set<String> keys = new ConcurrentHashSet<String>();
        BasicDBObject query = new BasicDBObject();
        
        DBCursor cursor = simulations.find(query);
        
        while (cursor.hasNext()) {
            keys.add((String) cursor.next().get("id"));
        }
        
        return keys;
    }
}
