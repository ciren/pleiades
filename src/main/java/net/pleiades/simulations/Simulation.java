/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations;

import java.util.List;
import java.util.Properties;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public interface Simulation {
    Task getUnfinishedTask();
    int getSamples();
    int addUnfinishedTask();
    int removeUnfinishedTask();
    int unfinishedCount();
    boolean hasUnfinishedTasks();
    boolean isComplete();
    boolean jobComplete();
    boolean completeTask(Task t, Properties p);
    int getSimulationNumber();
    String getID();
    String getJobID();
    String getOwner();
    String getOwnerEmail();
    String getOutputFileName();
    String getOutputPath();
    String getJobName();
    String getFileKey();
    void writeBinary(String fileName);
    void deleteBinary(String fileName);
    List<String> getResults();
    void setResults(List<String> results);
    Simulation emptyClone();
    String getCilibInput();
    String getReleaseType();
}
