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
        String workerID = message.getMessageObject();
        System.out.println("||| Request from " + workerID);
        
        if (runningMapStore.loadAll(runningMapStore.loadAllKeys()).containsValue(workerID)) {
            return;
        }
        
        if (simulationsMapStore.loadAllKeys().isEmpty()) {
            return;
        }

        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        Lock rLock = Hazelcast.getLock(Config.runningMap);
        
        rLock.lock();
        IMap<String, String> runningMap = Hazelcast.getMap(Config.runningMap);

        jLock.lock();
        IMap<String, List<Simulation>> jobsMap = Hazelcast.getMap(Config.simulationsMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();
        
        beat();

        String key = simulationSelector.getKey(simulationsMapStore);

        Task task = null;

        try {
            List<Simulation> collection = jobsMap.remove(key);
            if (collection == null) {
                throw new Exception("No simulations found for user: " + key);
            }

            for (Simulation s : collection) {
                task = s.getUnfinishedTask();
                if (task != null) {
                    break;
                }
            }

            jobsMap.put(key, collection);

            if (task == null) {
                throw new Exception("Unfinished task not found.");
            }

            runningMap.put(task.getId(), workerID);

            txn.commit();
            
            Map<String, Task> toPublish = new HashMap<String, Task>();
            toPublish.put(workerID, task);
            Config.TASKS_TOPIC.publish(toPublish);
            System.out.println("|||| Publishing task " + task.getId() + " to " + workerID);
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            jobsMap.forceUnlock(key);
            jLock.unlock();
            rLock.unlock();
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
