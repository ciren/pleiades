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
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.Transaction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import net.pleiades.database.RunningMapStore;
import net.pleiades.database.SimulationsMapStore;
import net.pleiades.simulations.Simulation;
import net.pleiades.simulations.selection.EqualProbabilitySelector;
import net.pleiades.simulations.selection.SimulationSelector;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class Distributor implements MessageListener<String>, Runnable {
    private final int CHECK_INTERVAL = 12;
    
    private static SimulationsMapStore simulationsMapStore;
    private static RunningMapStore runningMapStore;

    private SimulationSelector simulationSelector;
    private Properties properties;

    public Distributor() {
        this.properties = Config.getConfiguration();
        simulationsMapStore = new SimulationsMapStore();
        runningMapStore = new RunningMapStore();
        this.simulationSelector = new EqualProbabilitySelector();
    }

    public void activate() {
        Config.REQUESTS_TOPIC.addMessageListener(this);
        Config.TASKS_TOPIC.publish(Config.RESEND_REQUEST);

        Thread heartBeatThread = new Thread(this);
        heartBeatThread.setPriority(10);
        heartBeatThread.start();
    }

    @Override
    public synchronized void onMessage(Message<String> message) {
        long startTime = System.currentTimeMillis();
        boolean distributing = false;
        
        String workerID = message.getMessageObject();
        
        if (runningMapStore.loadAll(runningMapStore.loadAllKeys()).containsValue(workerID)) {
            return;
        }
        
        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        Lock rLock = Hazelcast.getLock(Config.runningMap);

        jLock.lock();
        IMap<String, List<Simulation>> jobsMap = Hazelcast.getMap(Config.simulationsMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        beat();
        Task task = null;

        try {
            String key = simulationSelector.getKey(simulationsMapStore);
        
            if (key.equals("")) {
                throw new Exception("No key found");
            }
            List<Simulation> collection = jobsMap.get(key);
            if (collection == null || collection.isEmpty()) {
                jobsMap.remove(key);
                jobsMap.forceUnlock(key);
                txn.commit();
                return;
            }
            Iterator<Simulation> iter = collection.iterator();
            
            while (iter.hasNext()) {
                Simulation s = iter.next();
                task = s.getUnfinishedTask();
                if (task != null) {
                    break;
                }
                else {
                    iter.remove();
                }
            }
            
            jobsMap.put(key, collection);
            jobsMap.forceUnlock(key);
            
            if (task == null) {
                txn.commit();
                return;
            }
            
            distributing = true;
            
            Map<String, Task> toPublish = new HashMap<String, Task>();
            toPublish.put(workerID, task);
            Config.TASKS_TOPIC.publish(toPublish);

            rLock.lock();
            IMap<String, String> runningMap = Hazelcast.getMap(Config.runningMap);
        
            runningMap.put(task.getId(), workerID);
            txn.commit();
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            if (distributing) {
                rLock.unlock();
            }
            jLock.unlock();
        }
    }

    @Override
    public void run() {
        int checkCounter = 0;
        while (true) {
            if (checkCounter == 0) {
                checkMembers();
            }

            beat();

            checkCounter = (checkCounter + 1) % CHECK_INTERVAL;

            Utils.sleep(5000);
        }
    }

    private void beat() {
        Config.HEARTBEAT_TOPIC.publish("beat");
    }

    private void checkMembers() {
        Lock rLock = Hazelcast.getLock(Config.runningMap);
        rLock.lock();

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        try {
            IMap<Task, String> runningMap = Hazelcast.getMap(Config.runningMap);

            for (Task task : runningMap.keySet()) {
                String workerID = runningMap.get(task);
                boolean memberIsAlive = false;

                for (Member m : Hazelcast.getCluster().getMembers()) {
                    if (Utils.getSocketStringFromWorkerID(workerID).equals(m.getInetSocketAddress().toString())) {
                        memberIsAlive = true;
                        break;
                    }
                }

                if (!memberIsAlive) {
                    regenerateTask(task.getParent());
                    runningMap.remove(task);
                    Utils.emailAdmin(Utils.getSocketStringFromWorkerID(workerID) + " Crashed!! One task has been recovered.", properties);
                }

                runningMap.forceUnlock(task);
            }

            txn.commit();
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            rLock.unlock();
        }
    }
    
    private void regenerateTask(Simulation sim) {
        //TODO: regenerate a task in the Simulations Map
        Utils.emailAdmin("Regenerating task for simulation " + sim.getID(), properties);
    }
}
