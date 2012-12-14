/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.base.RuntimeInterruptedException;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import net.pleiades.cluster.HazelcastCommunicator;
import net.pleiades.results.CilibSampleGatherer;
import net.pleiades.results.SampleGatherer;
import net.pleiades.simulations.Simulation;
import net.pleiades.tasks.Task;


public class ResultsListener implements EntryListener, MessageListener<Task> {
    private SampleGatherer gatherer;
    private Properties properties;

    public ResultsListener(Properties properties) {
        this.gatherer = new CilibSampleGatherer(properties);
        this.properties = properties;

        addListeners();
    }

    private void addListeners() {
        Config.RESULTS_TOPIC.addMessageListener(this);
    }

    public void execute() {
        HazelcastCommunicator cluster = new HazelcastCommunicator();
        cluster.connect();
        System.out.println("Now connected to Pleiades Cluster.\nWaiting for results...");

        Hazelcast.<String, Simulation>getMap(Config.completedMap).addEntryListener(this, true);
    }

    @Override
    public void entryAdded(EntryEvent event) {
        System.out.println("Entry added key=" + event.getKey() + ", value=" + event.getValue());
    }

    @Override
    public void entryRemoved(EntryEvent event) {
        System.out.println("Entry removed key=" + event.getKey() + ", value=" + event.getValue() + "\n");
    }

    @Override
    public void entryUpdated(EntryEvent event) {

    }

    @Override
    public void entryEvicted(EntryEvent event) {
        System.out.println("Entry evicted key=" + event.getKey() + ", value=" + event.getValue());
    }

    @Override
    public synchronized void onMessage(Message<Task> message) {
        if (message.getMessageObject().getId().equals("")) {
            System.out.println("Checking all results");
            checkAll();
            return;
        }

        System.out.println("Task completed:" + message.getMessageObject().getId());
        Task t = message.getMessageObject();

        Lock cLock = Hazelcast.getLock(Config.completedMap);
        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        cLock.lock();

        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        try {
            Simulation cSimulation = completedMap.get(t.getParent().getID());

            if (cSimulation == null) {
                Config.RESULTS_TOPIC.publish(t); //republish this completed task
                throw new Exception("No such simulation");
            }

            cSimulation.completeTask(t, properties);

            if (cSimulation.isComplete()) {
                completedMap.remove(t.getParent().getID());
                System.out.println("\nGathering: " + cSimulation.getID());
                System.out.println(cSimulation.getResults().size() + " tasks completed.");
                System.out.print("1");
                jLock.lock();
                IMap<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.simulationsMap);
                System.out.print("2");
                gatherer.gatherResults(simulationsMap, completedMap, cSimulation);
                simulationsMap.forceUnlock(cSimulation.getOwner());
                System.out.print("3");
                if (cSimulation.jobComplete()) {
                    Utils.emailUser(cSimulation, new File(properties.getProperty("email_complete_template")), properties, "");

                    IQueue<String> errors = Hazelcast.getQueue(Config.errorQueue);
                    IMap<String, byte[]> fileQueue = Hazelcast.getMap(Config.fileMap);

                    Iterator<String> iter = errors.iterator();

                    while (iter.hasNext()) {
                        if (iter.next().startsWith(cSimulation.getJobID())) {
                            iter.remove();
                        }
                    }
                    System.out.println("Error Queue Size: " + errors.size());

                    fileQueue.remove(cSimulation.getFileKey());
                    fileQueue.forceUnlock(cSimulation.getFileKey());
                    System.out.println("File Queue Size: " + fileQueue.size());
                }
            } else {
                System.out.println("here");
                completedMap.put(t.getParent().getID(), cSimulation);
            }
            completedMap.forceUnlock(t.getParent().getID());
            System.out.print("4");
            txn.commit();
            System.out.print("5");
        } catch (RuntimeInterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            System.out.println("ERROR:");
            e.printStackTrace();
            txn.rollback();
        } finally {
            jLock.unlock();
            cLock.unlock();
        }
    }

    private void checkAll() {
        Lock cLock = Hazelcast.getLock(Config.completedMap);
        //Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        cLock.lock();

        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedMap);
        Transaction txn = Hazelcast.getTransaction();

        for (Simulation cSimulation : completedMap.values()) {
            txn.begin();
            try {
                if (cSimulation.isComplete()) {
                    completedMap.remove(cSimulation.getID());
                    System.out.println("\nGathering: " + cSimulation.getID());
                    System.out.println(cSimulation.getResults().size() + " tasks completed.");
//                    jLock.lock();
//                    IMap<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.simulationsMap);
                    gatherer.gatherResults(null, completedMap, cSimulation);
//                    simulationsMap.forceUnlock(cSimulation.getOwner());
                } else {
                    System.out.println("here");
                    completedMap.put(cSimulation.getID(), cSimulation);
                }
                completedMap.forceUnlock(cSimulation.getID());
                txn.commit();
            } catch (RuntimeInterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable e) {
                System.out.println("ERROR:");
                e.printStackTrace();
                txn.rollback();
            } finally {
                //jLock.unlock();
                cLock.unlock();
            }
        }
    }
}
