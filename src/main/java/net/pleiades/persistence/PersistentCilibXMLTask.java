 /* Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.persistence;

import com.mongodb.BasicDBObject;
import net.pleiades.tasks.Task;

/**
 *
 * @author bennie
 */
public class PersistentCilibXMLTask extends BasicDBObject {

    public PersistentCilibXMLTask(Task t) {
        put("cilibInput", t.getInput());
        put("id", t.getId());
    }
    
    public String cilibInput() {
        return (String) get("cilibInput");
    }
    
    public String id() {
        return (String) get("id");
    }
    
}
