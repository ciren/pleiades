/* Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.persistence;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import net.pleiades.simulations.CilibSimulation;
import net.pleiades.simulations.Simulation;

/**
 *
 * @author bennie
 */
public class PersistentSimulationsList extends BasicDBObject {

    public PersistentSimulationsList() {
    }

    public PersistentSimulationsList(String user, List<Simulation> list) {
        put("owner", user);
        
        ArrayList<DBObject> array = new ArrayList<DBObject>();
        for (Simulation s : list) {
            array.add(new PersistentSimulationsMapObject(s));
        }
        
        put("simulations", array);
    }
    
    public String owner() {
        return (String) get("owner");
    }

    public List<Simulation> simulations() {
        ArrayList<DBObject> array = (ArrayList<DBObject>) get("simulations");
        LinkedList<Simulation> list = new LinkedList<Simulation>();
        
        for (DBObject o : array) {
            list.add(new CilibSimulation((PersistentSimulationsMapObject) o));
        }
        
        return list;
    }
}
