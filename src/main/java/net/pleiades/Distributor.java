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
import net.pleiades.simulations.MockSimulation;
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
    private final int RESEND_REQUEST_INTERVAL = 3;

    private SimulationSelector simulationSelector;
    private Properties properties;

    public Distributor(Properties p) {
        this.properties = p;
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

        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        Lock rLock = Hazelcast.getLock(Config.runningMap);
        jLock.lock();
        rLock.lock();

        IMap<String, List<Simulation>> jobsMap = Hazelcast.getMap(Config.simulationsMap);
        IMap<String, Simulation> runningMap = Hazelcast.getMap(Config.runningMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        if (jobsMap.isEmpty()) {
            txn.rollback();
            rLock.unlock();
            jLock.unlock();
            return;
        }
        beat();

        String key = simulationSelector.getKey(jobsMap);

        try {
            runningMap.put(workerID, new MockSimulation());

            List<Simulation> collection = jobsMap.remove(key);
            if (collection == null) {
                throw new Exception("Simulation " + key + "not found.");
            }

            Task task = null;
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

            runningMap.put(workerID, task.getParent());

            txn.commit();

            Map<String, Task> toPublish = new HashMap<String, Task>();
            toPublish.put(workerID, task);
            Config.TASKS_TOPIC.publish(toPublish);
            System.out.println("|||| Publishing task " + task.getId() + " to " + workerID);
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            jobsMap.forceUnlock(key);
            runningMap.forceUnlock(workerID);
            rLock.unlock();
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

            if (checkCounter % RESEND_REQUEST_INTERVAL == 0) {
                Config.TASKS_TOPIC.publish(Config.RESEND_REQUEST);
            }

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
            IMap<String, Simulation> runningMap = Hazelcast.getMap(Config.runningMap);

            for (String key : runningMap.keySet()) {
                Simulation sim = runningMap.get(key);
                boolean memberIsAlive = false;

                if (!sim.getID().equals("Mock")) {
                    for (Member m : Hazelcast.getCluster().getMembers()) {
                        if (Utils.getSocketStringFromWorkerID(key).equals(m.getInetSocketAddress().toString())) {
                            memberIsAlive = true;
                            break;
                        }
                    }

                    if (!memberIsAlive) {
                        sim.addUnfinishedTask();
                        runningMap.remove(key);
                        Utils.emailAdmin(Utils.getSocketStringFromWorkerID(key) + " Crashed!! One task has been recovered.", properties);
                    }
                }

                runningMap.forceUnlock(key);
            }

            txn.commit();
        } catch (Throwable e) {
            txn.rollback();
        } finally {
            rLock.unlock();
        }
    }
}
