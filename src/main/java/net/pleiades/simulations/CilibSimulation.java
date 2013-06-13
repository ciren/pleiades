/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.simulations;

import com.hazelcast.core.Hazelcast;
import com.mongodb.BasicDBObject;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import net.pleiades.Config;
import net.pleiades.database.CompletedMapStore;
import net.pleiades.persistence.PersistentCompletedMapObject;
import net.pleiades.persistence.PersistentSimulationsMapObject;
import net.pleiades.tasks.CilibXMLTask;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class CilibSimulation extends BasicDBObject implements Simulation, Serializable {
    private static final long serialVersionUID = 778767591595073953L;

    private String cilibInput, fileKey;
    private String outputFileName, outputPath;
    private int samples;
    private String owner, ownerEmail, id, jobName;
    private int unfinishedTasks;
    private String releaseType;
    private List<String> results;

    public CilibSimulation(String cilibInput, String fileKey/*byte[] jar*/, String outputFileName, String outputPath, int samples,
            String owner, String ownerEmail, String id, String name, String releaseType) {
        this.owner = owner;
        this.ownerEmail = ownerEmail;
        this.jobName = name;
        this.cilibInput = cilibInput;
        this.fileKey = fileKey;
        this.outputFileName = outputFileName;
        this.outputPath = outputPath;
        this.samples = samples;
        this.unfinishedTasks = samples;
        this.releaseType = releaseType;
        this.results = new LinkedList<String>();

        if (!createID(id)) {
            System.out.println("Error: You may not run more than " + Config.MaxJobsPerUser + " jobs at a time.");
            System.exit(1);
        }
        
        this.fileKey = getJobID();
    }

    public CilibSimulation(CilibSimulation other) {
        this.owner = other.owner;
        this.ownerEmail = other.ownerEmail;
        this.jobName = other.jobName;
        this.samples = other.samples;
        this.id = other.id;
        this.outputFileName = other.outputFileName;
        this.outputPath = other.outputPath;
        this.results = new LinkedList<String>();
        this.fileKey = other.fileKey;
        this.unfinishedTasks = other.unfinishedTasks;
        this.cilibInput = other.cilibInput;
        this.releaseType = other.releaseType;
    }

    public CilibSimulation(PersistentCompletedMapObject p) {
        this.owner = p.owner();
        this.ownerEmail = p.ownerEmail();
        this.jobName = p.jobName();
        this.samples = p.samples();
        this.id = p.id();
        this.outputFileName = p.outputFileName();
        this.outputPath = p.outputPath();
        this.results = p.results();
        this.fileKey = p.fileKey();
        this.releaseType = p.releaseType();
        this.unfinishedTasks = p.unfinishedTasks();

        this.cilibInput = p.cilibInput();
    }
    
    public CilibSimulation(PersistentSimulationsMapObject p) {
        this.owner = p.owner();
        this.ownerEmail = p.ownerEmail();
        this.jobName = p.jobName();
        this.samples = p.samples();
        this.id = p.id();
        this.outputFileName = p.outputFileName();
        this.outputPath = p.outputPath();
        this.results = p.results();
        this.fileKey = p.fileKey();
        this.releaseType = p.releaseType();
        this.unfinishedTasks = p.unfinishedTasks();

        this.cilibInput = p.cilibInput();
    }

    @Override
    public Simulation emptyClone() {
        return new CilibSimulation(this);
    }

    @Override
    public boolean isComplete() {
        return (samples == results.size());
    }

    @Override
    public boolean jobComplete() {
        Map<String, Simulation> completedMap = Hazelcast.getMap(Config.completedMap);
        Collection<Simulation> simulations = completedMap.values();

        for (Simulation s : simulations) {
            if (s.getJobID().equals(getJobID())) {
                return false;
            }
        }

        return true;
    }

    private boolean createID(String id) {
        CompletedMapStore store = new CompletedMapStore();
        Collection<String> collection = store.loadAllKeys();

        String myId;
        boolean validId;

        for (int i = 0; i < Config.MaxJobsPerUser; i++) {
            myId = id.replaceAll("_", "_" + i + "_");

            validId = true;
            for (String s : collection) {
                if (s.substring(0, s.lastIndexOf("_")).equals(myId.substring(0, myId.lastIndexOf("_")))) {
                    validId = false;
                    break;
                }
            }

            if (validId) {
                this.id = myId;
                return true;
            }
        }

        return false;
    }

    @Override
    public String getID() {
        return id;
    }

    @Override
    public final String getJobID() {
        return id.substring(0, id.lastIndexOf("_"));
    }

    @Override
    public String getOwner() {
        return owner;
    }

    @Override
    public String getOwnerEmail() {
        return ownerEmail;
    }

    @Override
    public String getJobName() {
        return jobName;
    }

    @Override
    public boolean completeTask(Task t, Properties properties) {
        String path = properties.getProperty("gather_results_folder") + owner + "/" + jobName + "/temp/";
        File temp = new File(path);
        temp.mkdirs();
        path = path + t.getId() + ".tmp";

        while ((temp = new File(path)).exists()) {
            path = path.replaceAll(".tmp", "_.tmp");
        }

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(temp));

            writer.append(t.getResults());
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return results.add(path);
    }

    @Override
    public synchronized boolean hasUnfinishedTasks() {
        return unfinishedTasks > 0;
    }

    @Override
    public synchronized Task getUnfinishedTask() {
        if (!hasUnfinishedTasks()) {
            return null;
        } else {
            unfinishedTasks--;
        }

        return CilibXMLTask.of(cilibInput, id + "_" + String.valueOf(samples - unfinishedTasks), this);
    }

    @Override
    public void writeBinary(String fileName) {
        try {
            File binary = new File(fileName);
            binary.setExecutable(true, false);
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(binary));

            Map<String, byte[]> fileQueue = Hazelcast.getMap(Config.fileMap);
            byte[] bytes = fileQueue.get(fileKey);
            
            writer.write(bytes);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteBinary(String fileName) {
        File binary = new File(fileName);
        binary.delete();
    }

    @Override
    public List<String> getResults() {
        return results;
    }

    @Override
    public int getSamples() {
        return samples;
    }

    @Override
    public String getOutputFileName() {
        return outputFileName;
    }

    @Override
    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public int addUnfinishedTask() {
        return ++unfinishedTasks;
    }

    @Override
    public int removeUnfinishedTask() {
        return --unfinishedTasks;
    }

    @Override
    public int unfinishedCount() {
        return unfinishedTasks;
    }

    @Override
    public int getSimulationNumber() {
        return Integer.parseInt(id.substring(id.lastIndexOf("_") + 1)) + 1;
    }

    @Override
    public String getFileKey() {
        return fileKey;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    @Override
    public String getCilibInput() {
        return cilibInput;
    }

    @Override
    public String toString() {
        return "Job: " + jobName + " (" + outputFileName + ")";
    }

    @Override
    public void setResults(List<String> results) {
        this.results = results;
    }

    @Override
    public String getReleaseType() {
        return releaseType;
    }
}
