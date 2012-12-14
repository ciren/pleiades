/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Transaction;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import net.pleiades.simulations.MockSimulation;
import net.pleiades.simulations.Simulation;
import net.pleiades.tasks.CilibXMLTask;

/**
 *
 * @author bennie
 */
public class Gatherer {
    Properties properties;

    public Gatherer(Properties p) {
        this.properties = p;
    }

    public void start(boolean cont) {
        Utils.authenticate(properties, "admin");
        new ResultsListener(properties).execute();
        new ErrorListener(properties).exexcute();

        if (cont) {
            continueSimulations();
            Config.RESULTS_TOPIC.publish(CilibXMLTask.of("", "", new MockSimulation()));
        }
    }

    private void continueSimulations() {
        System.out.println("Continue...");
        Lock cLock = Hazelcast.getLock(Config.completedMap);
        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        cLock.lock();

        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();
        try {
            if (!completedMap.isEmpty()) {
                System.out.println("Found " + completedMap.size() + " unfinished simulations!");

                jLock.lock();
                IMap<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.simulationsMap);
                Map<String, List<Simulation>> jobs = new HashMap();

                for (Simulation current : completedMap.values()) {
                    Simulation s = current;
                    System.out.println("Unfinished simulation: " + s.getID());

                    int completed = 0;
                    String line;
                    InputStream inputStream;
                    BufferedReader reader;

                    String path = "/SAN/working/pleiades/results/" + s.getOwner() + "/" + s.getJobName() + "/temp/";

                    List<String> command = new ArrayList<String>();
                    command.add("ls");
                    command.add(path);
                    command.add("-1a");

                    List<String> files = new ArrayList<String>();
                    try {
                        Process shell = new ProcessBuilder(command).start();
                        inputStream = shell.getInputStream();
                        reader = new BufferedReader(new InputStreamReader(inputStream));

                        while ((line = reader.readLine()) != null) {
                            if (line.contains(s.getID())) {
                                files.add(path + line);
                            }
                        }

                        reader.close();
                        inputStream.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    s.setResults(files);

                    completedMap.put(s.getID(), s);

                    //add the recovered sims to the jobs list?
                    // -calculate outstanding sample count
                    // -set unfinishedTasks
                    // -add to jobs queue
                }

                txn.commit();
            } else {
                txn.rollback();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            cLock.unlock();
            jLock.unlock();
        }
    }
}
