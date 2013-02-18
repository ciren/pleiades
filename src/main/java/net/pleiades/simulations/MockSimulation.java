/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class MockSimulation implements Simulation, Serializable {

    @Override
    public void setResults(List<String> results) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Task getUnfinishedTask() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getSamples() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int addUnfinishedTask() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int removeUnfinishedTask() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int unfinishedCount() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasUnfinishedTasks() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isComplete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean jobComplete() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean completeTask(Task t, Properties p) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getSimulationNumber() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getID() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getJobID() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getOwner() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getOwnerEmail() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getOutputFileName() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getOutputPath() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getJobName() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getFileKey() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void writeBinary(String fileName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void deleteBinary(String fileName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<String> getResults() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Simulation emptyClone() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getCilibInput() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getReleaseType() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
