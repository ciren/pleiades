/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.database;

import java.util.ArrayList;
import junit.framework.Assert;
import net.pleiades.simulations.CilibSimulation;
import net.pleiades.simulations.Simulation;

/**
 * NB! These tests will only pass if a valid configuration file is provided
 * in the running directory and the persistent database is correctly configured.
 * 
 * @author bennie
 */
public class TestPersistentMapStores {
    public void testSimulationsMapStore() {
        SimulationsMapStore store = new SimulationsMapStore();
        
        Simulation s = new CilibSimulation("", "", "", "", 0, "", "", "test_0", "", "");
        s.setResults(new ArrayList<String>());
        
        String id = s.getID();
        
        store.store("key", s);
        Simulation loaded = store.load(id);
        
        Assert.assertEquals(id, loaded.getID());
        Assert.assertNull(loaded.getResults());
        
        store.delete(id);
        loaded = store.load(id);
        
        Assert.assertNull(loaded);
    }
    
    public void testCompletedMapStore() {
        CompletedMapStore store = new CompletedMapStore();
        
        Simulation s = new CilibSimulation("", "", "", "", 0, "", "", "test_0", "", "");
        s.setResults(new ArrayList<String>());
        
        String id = s.getID();
        
        store.store("key", s);
        Simulation loaded = store.load(id);
        
        Assert.assertEquals(id, loaded.getID());
        Assert.assertNotNull(loaded.getResults());
        
        store.delete(id);
        loaded = store.load(id);
        
        Assert.assertNull(loaded);
    }
    
    public void testRunningMapStore() {
        RunningMapStore store = new RunningMapStore();
                
        store.store("test_id", "test_worker_id");
        
        String worker = store.load("test_id");
        
        Assert.assertEquals("test_worker_id", worker);
        
        store.delete("test_id");
        
        worker = store.load("test_id");
        
        Assert.assertNull(worker);
    }
    
    public void testFileMapStore() {
        FileMapStore store = new FileMapStore();
        
        byte[] bytes = new byte[]{0,127,-128};
        
        store.store("test_key", bytes);
        
        byte[] loaded = store.load("test_key");
        
        Assert.assertEquals(loaded[0], bytes[0]);
        Assert.assertEquals(loaded[1], bytes[1]);
        Assert.assertEquals(loaded[2], bytes[2]);
        
        store.delete("test_key");
        
        loaded = store.load("test_key");
        
        Assert.assertNull(loaded);
    }
}
