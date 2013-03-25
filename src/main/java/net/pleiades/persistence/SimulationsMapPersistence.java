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
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.pleiades.database.SimulationsMapStore;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class SimulationsMapPersistence implements MapStore<String, List<Simulation>> {
    private static SimulationsMapStore store;
    
    public SimulationsMapPersistence() throws Exception {
        store = new SimulationsMapStore();
        if (!store.connect()) {
            System.out.println("ERROR: Unable to connect to persistent store. Contact administrator.");
            System.exit(1);
        }
        System.out.println("Connected to simulations store");
    }
    
    @Override
    public void store(String k, List<Simulation> v) {
        store.store(k, v);
    }

    @Override
    public void storeAll(Map<String, List<Simulation>> map) {
        store.storeAll(map);
    }

    @Override
    public void delete(String k) {
        store.delete(k);
    }

    @Override
    public void deleteAll(Collection<String> clctn) {
        store.deleteAll(clctn);
    }

    @Override
    public List<Simulation> load(String k) {
        return store.load(k);
    }

    @Override
    public Map<String, List<Simulation>> loadAll(Collection<String> clctn) {
        return store.loadAll(clctn);
    }

    @Override
    public Set<String> loadAllKeys() {
        return store.loadAllKeys();
    }
}
