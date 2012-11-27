/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.persistence;

import com.hazelcast.core.MapStore;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import net.pleiades.database.MongoCommunicator;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class CompletedMapPersistence implements MapStore<String, Simulation> {
    private static PersistentStoreCommunicator store;
    private static Properties properties;

    public CompletedMapPersistence() throws Exception {
        store = new MongoCommunicator(properties);
        if (!store.connect()) {
            System.out.println("ERROR: Unable to connect to persistent store. Contact administrator.");
            System.exit(1);
        }
    }

    @Override
    public void store(String k, Simulation v) {
        store.store(new PersistentCilibSimulation(v));
    }

    @Override
    public void storeAll(Map<String, Simulation> map) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(String k) {
        store.delete(k);
    }

    @Override
    public void deleteAll(Collection<String> clctn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Simulation load(String k) {
        return store.load(k);
    }

    @Override
    public Map<String, Simulation> loadAll(Collection<String> clctn) {
        return store.loadAll(clctn);
    }

    @Override
    public Set<String> loadAllKeys() {
        return store.loadAllKeys();
    }
    
    public static void setProperties(Properties p) {
        CompletedMapPersistence.properties = p;
    }
}
