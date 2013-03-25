/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations.selection;

import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.management.RuntimeOperationsException;
import net.pleiades.persistence.SimulationsMapPersistence;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class EqualProbabilitySelector implements SimulationSelector {

    @Override
    public String getKey(Map<String, List<Simulation>> jobs) {
        //RandomProvider random = new MersenneTwister();
        Random random = new Random();
        
        SimulationsMapPersistence simulationsDB;
                
        try {
            simulationsDB = new SimulationsMapPersistence();
        } catch (Exception e) {
            throw new Error("Unable to connect to persistent store. Aborting.");
        }

        //Object[] keySet = jobs.keySet().toArray();
        Object[] keySet = simulationsDB.loadAllKeys().toArray();
        int keys = keySet.length;
        double rand = random.nextDouble();

        int selected = (int)Math.ceil(rand * keys) - 1;

        //System.out.println("\nJobs: " + jobs.size() + "\nKeys: " + keys + "\nSelected: " + selected + "(" + (String)keySet[selected] + ")");
        
        return (String)keySet[selected];
    }
}
