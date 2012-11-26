/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks;

import java.util.Properties;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public interface Task {
    boolean execute(Properties p);
    String getResults();
    String getOutput();
    String getProgress();
    String getId();
    String getInput();
    void writeFile();
    void deleteFile();
    Simulation getParent();
}
