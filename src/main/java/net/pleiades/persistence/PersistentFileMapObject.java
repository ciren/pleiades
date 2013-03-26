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
public class PersistentFileMapObject extends BasicDBObject {

    public PersistentFileMapObject() {
    }
    
    public PersistentFileMapObject(String file_key, byte[] file) {
        put("file_key", file_key);
        put("file", file);
    }
    
    public String fileKey() {
        return (String) get("file_key");
    }
    
    public byte[] file() {
        return (byte[]) get("file");
    }
}
