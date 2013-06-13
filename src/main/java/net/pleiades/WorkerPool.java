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
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import jline.ConsoleReader;
import net.pleiades.tasks.executor.TaskExecutor;

public class WorkerPool {

    private Lock dLock;

    private Properties properties;
    private TaskExecutor[] workers;
    private ConsoleReader con;
    private boolean quiet;
    TaskDistributor distributor = null;
    State state;

    public WorkerPool(int count, boolean quiet) {
        this.properties = Config.getConfiguration();
        
        try {
            this.workers = new TaskExecutor[count];
            this.con = new ConsoleReader();
            
            for (int i = 0; i < count; i++) {
                workers[i] = new TaskExecutor(String.valueOf(i));
            }

            this.quiet = quiet;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void execute() {
        Thread dThread = null;
        
        if (Utils.silentAuthenticate("distributor", properties.getProperty("distributor_password"))) {
            distributor = new TaskDistributor(dLock);
            dThread = new Thread(distributor);
            dThread.setPriority(10);
            dThread.start();
        }
        
        try {
            for (int i = 0; i < workers.length; i++) {
                System.out.println("Starting worker " + i);
                new Thread(workers[i]).start();
            }

            state = State.WORKING;
            new Thread(new Reader()).start();

            while (true) {
                Utils.sleep(200);
                if (!quiet) {
                    con.clearScreen();

                    if (dThread != null && distributor.getDistributor() != null) {
                        System.out.println(Utils.header
                                .replace("|___/", "|___/ * " + state.name() + " * (Enter to toggle)")
                                .replace("|__ \\", "|__ \\ * Distributor *"));
                    } else {
                        System.out.println(Utils.header
                                .replace("|___/", "|___/ * " + state.name() + " * (Enter to toggle)"));
                    }

                    for(int i = 0; i < workers.length; i++) {
                        System.out.println("Worker " + i + ": " + workers[i].getStateString() + "\n");
                    }
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private class Reader implements Runnable {
        @Override
        public void run() {
            while(true) {
                try {
                    if (con.getInput().available() != 0) {
                        int key = con.readVirtualKey();

                        switch (key) {
                            case 10:
                                for (int i = 0; i < workers.length; i++) {
                                    workers[i].toggle();
                                }

                                if (state == State.WORKING) {
                                    state = State.PAUSED;
                                } else {
                                    state = State.WORKING;
                                }
                                break;
                                
                            case 27:
                                if (!quiet) {
                                    quiet = true;
                                } else {
                                    quiet = false;
                                }
                                break;

                            default:
                                //System.out.println(key);
                        }
                    }

                    Utils.sleep(10);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private class TaskDistributor implements Runnable, MessageListener<String> {
        private Distributor distributor = null;
        private boolean isAlive;
        private Lock lock;

        public TaskDistributor(Lock lock) {
            this.lock = lock;
            this.isAlive = false;
            addListeners();
        }

        private void addListeners() {
            Config.HEARTBEAT_TOPIC.addMessageListener(this);
        }

        @Override
        public void run() {
            while(distributor == null) {
                if (!isAlive) {
                    lock = Hazelcast.getLock(Config.distributor);

                    boolean locked = false;

                    try {
                        locked = lock.tryLock(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Utils.emailAdmin(Hazelcast.getCluster().getLocalMember().getInetSocketAddress() +
                                " Crashed while waiting for distributor lock! Workers have been terminated.",
                                properties);
                        System.exit(1);
                    }

                    if (locked) {
                        distributor = new Distributor();
                        distributor.activate();
                        Utils.emailAdmin("Pleiades Distributor now running on: "
                                + Hazelcast.getCluster().getLocalMember().getInetSocketAddress(),
                                properties);
                    }
                }
                isAlive = false;
                Utils.sleep(30000);
            }
        }

        public Distributor getDistributor() {
            return distributor;
        }

        @Override
        public void onMessage(Message<String> message) {
            isAlive = true;
        }
    }
}
