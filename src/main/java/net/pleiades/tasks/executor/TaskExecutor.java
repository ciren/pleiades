/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.tasks.executor;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import java.util.Map;
import java.util.Properties;
import net.pleiades.Config;
import net.pleiades.State;
import static net.pleiades.State.*;
import net.pleiades.Utils;
import net.pleiades.tasks.Task;

/**
 * @author bennie
 */
public class TaskExecutor implements Executor, Runnable, MessageListener<Map<String, Task>> {

    protected State state;
    protected Task currentTask;
    protected boolean running;
    protected Properties properties;
    protected String id;

    public TaskExecutor(Properties properties, String id) {
        this.id = Hazelcast.getCluster().getLocalMember().getInetSocketAddress().toString() + "-" + id;
        this.state = IDLE;
        this.running = false;
        this.properties = properties;

        addListeners();
    }

    @Override
    public synchronized void requestNewTask() {
        state(REQUESTING);

        Config.REQUESTS_TOPIC.publish(id);

        state(REQUEST_SENT);
    }

    public void executeTask() {
        state(PREPARING);

        String bin = currentTask.getId() + ".run";
        currentTask.getParent().writeBinary(bin);
        currentTask.writeFile();

        state(EXECUTING);

        boolean success = currentTask.execute(properties);
        currentTask.getParent().deleteBinary(bin);
        currentTask.deleteFile();

        state(COMPLETING);

        if (success) {
            Config.RESULTS_TOPIC.publish(currentTask);
        } else if (!currentTask.getOutput().isEmpty()) {
            Config.ERRORS_TOPIC.publish(currentTask);
        }

        state(COMPLETED);

        currentTask = null;

        state(IDLE);
    }

    @Override
    public synchronized void onMessage(Message<Map<String, Task>> message) {
        if (!message.getMessageObject().isEmpty()) {
            if (message.getMessageObject().keySet().contains(id)) {
                currentTask = message.getMessageObject().get(id);
                System.out.println("|| " + id + " got " + currentTask.getId());
                state(JOB_RECEIVED);
            }
        } else {
            if (isState(REQUEST_SENT)) {
                state(IDLE);
            }
        }
    }

    @Override
    public void run() {
        running = true;

        while (true) {
            Utils.sleep(5000);

            if (currentTask != null) {
                executeTask();
            } else if (running && !isState(REQUEST_SENT)) {
                requestNewTask();
            }
        }
    }

    @Override
    public String getStateString() {
        String taskName, progress;
        if (currentTask == null || isState(REQUESTING) || isState(IDLE)) {
            taskName = "(No task)";
            progress = "(No progress)";
        } else {
            taskName = currentTask.getId();
            progress = currentTask.getProgress();
        }

        return state.name() + " " + taskName + "\n" + progress;
    }

    @Override
    public void stop() {
        removeListeners();
        this.running = false;
    }

    @Override
    public void start() {
        addListeners();
        this.running = true;
    }

    @Override
    public void toggle() {
        if (running) {
            stop();
        } else {
            start();
        }
    }

    private void addListeners() {
        Config.TASKS_TOPIC.addMessageListener(this);
    }

    private void removeListeners() {
        Config.TASKS_TOPIC.removeMessageListener(this);
    }

    private boolean isState(State s) {
        return state == s;
    }

    private void state(State s) {
        state = s;
        System.out.println("|| " + id + " " + s);
    }
}
