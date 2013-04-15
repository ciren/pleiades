/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import java.util.Properties;

/**
 *
 * @author bennie
 */
public class Gatherer {
    Properties properties;

    public Gatherer() {
        this.properties = Config.getConfiguration();
    }

    public void start(boolean cont) {
        Utils.authenticate("admin");
        new ResultsListener().execute();
        new ErrorListener().exexcute();
    }
}
