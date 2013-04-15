/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.results;

import com.hazelcast.core.IMap;
import java.util.List;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface SampleGatherer {
    //Simulation resultsAvailable();
    Simulation gatherResults(IMap<String, Simulation> completedMap, Simulation s) throws Throwable;
}
