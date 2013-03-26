/* Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.persistence;

import com.mongodb.BasicDBObject;

/**
 *
 * @author bennie
 */
public class PersistentRunningMapObject extends BasicDBObject {

    public PersistentRunningMapObject() {    
    }
    
    public PersistentRunningMapObject(String taskID, String workerID) {
        put("task_id", taskID);
        put("worker_id", workerID);
    }
    
    public String taskID() {
        return (String) get("task_id");
    }
    
    public String workerID() {
        return (String) get("worker_id");
    }
}
