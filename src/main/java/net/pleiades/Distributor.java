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
import com.hazelcast.core.ITopic;
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
import net.pleiades.simulations.MockSimulation;
import net.pleiades.simulations.Simulation;
import net.pleiades.simulations.selection.EqualProbabilitySelector;
import net.pleiades.simulations.selection.SimulationSelector;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class Distributor implements EntryListener, MessageListener<String> {
    private ITopic requestTopic, tasksTopic;
    //private List<String> requests;
    private SimulationSelector simulationSelector;
    private HeartBeat heartBeat;
    private Properties properties;

    public Distributor(Properties p) {
        properties = p;
        simulationSelector = new EqualProbabilitySelector();
        requestTopic = Hazelcast.getTopic(Config.requestTopic);
        tasksTopic = Hazelcast.getTopic(Config.tasksTopic);
        //requests = new LinkedList<String>();
        heartBeat = new HeartBeat();
    }

    public void activate() {
        requestTopic.addMessageListener(this);
        Hazelcast.<String, Simulation>getMap(Config.simulationsMap).addEntryListener(this, true);

        tasksTopic.publish(new HashMap<String, Task>());
        Thread heartBeatThread = new Thread(heartBeat);
        heartBeatThread.setPriority(10);
        heartBeatThread.start();
    }

    @Override
    public synchronized void onMessage(Message<String> message) {
        System.out.println("Request from " + message.getMessageObject());
        String messageObject = message.getMessageObject();
        
        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        jLock.lock();
        
        IMap<String, List<Simulation>> jobsMap = Hazelcast.getMap(Config.simulationsMap);
        IMap<String, Simulation> runningMap = Hazelcast.getMap(Config.runningMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();
        
        Task t = null;
        String workerID = messageObject;
        
        if (jobsMap.size() == 0) {
            txn.rollback();
            jLock.unlock();
            //requests.add(messageObject);
            return;
        }
        heartBeat.beat();
        
        String key = simulationSelector.getKey(jobsMap);
        
        try {
            runningMap.put(workerID, new MockSimulation());
            
            List<Simulation> collection = jobsMap.remove(key);
            if (collection == null) {
                txn.rollback();
                jLock.unlock();
                //requests.add(messageObject);
                return;
            }
            
            for (Simulation s : collection) {
                t = null;
                if ((t = s.getUnfinishedTask()) != null) {
                    break;
                }
            }
            
            jobsMap.put(key, collection);

            if (t == null) {
                txn.rollback();
                jLock.unlock();
                //requests.add(messageObject);
                return;
            }
            
            runningMap.put(workerID, t.getParent());

            txn.commit();
        } catch (Throwable e) {
            e.printStackTrace();
            txn.rollback();
        } finally {
            jobsMap.forceUnlock(key);
            runningMap.forceUnlock(workerID);
            jLock.unlock();
        }

        Map<String, Task> toPublish = new HashMap<String, Task>();
        toPublish.put(message.getMessageObject(), t);

        //System.out.println("Publishing task " + t.getId() + " to " + message.getMessageObject());
        tasksTopic.publish(toPublish);
    }

    @Override
    public void entryAdded(EntryEvent event) {
        
    }

    @Override
    public void entryEvicted(EntryEvent event) {
        
    }

    @Override
    public void entryRemoved(EntryEvent event) {
        
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        
    }

    private class HeartBeat implements Runnable {
        final int INTERVAL = 12;
        ITopic heartBeatTopic;
        int checkCounter;

        public HeartBeat() {
            checkCounter = INTERVAL;
            heartBeatTopic = Hazelcast.getTopic(Config.heartBeatTopic);
        }

        @Override
        public void run() {
            while (true) {
                if (checkCounter == 0) {
                    checkMembers();
                    checkCounter = INTERVAL;
                } else {
                    checkCounter--;
                }
            
                heartBeatTopic.publish("beat");
                tasksTopic.publish(new HashMap<String, Task>());
                Utils.sleep(5000);
            }
        }

        public void beat() {
            heartBeatTopic.publish("beat");
        }
        
        private void checkMembers() {
            Lock jLock = Hazelcast.getLock(Config.simulationsMap);
            Lock cLock = Hazelcast.getLock(Config.completedMap);
            jLock.lock();
            cLock.lock();
            
            Transaction txn = Hazelcast.getTransaction();
            txn.begin();
            
            try {
                IMap<String, Simulation> runningMap = Hazelcast.getMap(Config.runningMap);
                
                Iterator<String> iter = runningMap.keySet().iterator();
                while (iter.hasNext()) {
                    String key = iter.next();
                    boolean memberIsAlive = false;
                    
                    Simulation sim = runningMap.get(key);
                    
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
                cLock.unlock();
                jLock.unlock();
            }
        }
    }
}
