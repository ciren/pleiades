/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.results;

import com.hazelcast.core.IMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.pleiades.simulations.Simulation;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author bennie
 */
public class CilibSampleGatherer implements SampleGatherer {
    
    private Properties properties;

    public CilibSampleGatherer(Properties properties) {
        this.properties = properties;
    }

    @Override
    public synchronized Simulation gatherResults(IMap<String, List<Simulation>> simulationsMap, IMap<String, Simulation> completedMap, Simulation s) throws Throwable {
        if (simulationsMap != null) {
            completedMap.remove(s.getID());
            
            List<Simulation> sims = simulationsMap.remove(s.getOwner());
            Iterator<Simulation> iter = sims.iterator();

            while (iter.hasNext()) {
                if (iter.next().getID().equals(s.getID())) {
                    iter.remove();
                    break;
                }
            }
            if (!sims.isEmpty()) {
                simulationsMap.put(s.getOwner(), sims);
            }
        }

        List<String> results;
        
        results = s.getResults();

        BufferedReader reader;
        StringBuilder thisResult, resultsBuilder = new StringBuilder();
        String thisResultString;
        String[] measurements = new String[]{};
        boolean gotHeaders = false;
        int linesPerSample = 0;
        
        for (String r : results) {
            thisResult = new StringBuilder();
            reader = new BufferedReader(new FileReader(new File(r)));
        
            while (reader.ready()) {
                thisResult.append(reader.readLine());
                thisResult.append("\n");
            }
            
            thisResultString = thisResult.toString();
            
            if (!gotHeaders) {
                measurements = getMeasurements(thisResultString);
                resultsBuilder.append(constructHeaders(measurements, results.size()));
                linesPerSample = StringUtils.countMatches(thisResultString.substring(thisResultString.lastIndexOf("(0)") + 3), "\n") - 1;
                
                gotHeaders = true;
            }
                    
            resultsBuilder.append(thisResultString.substring(thisResultString.lastIndexOf("(0)") + 3));
        }

        String jobName = s.getJobName();
        String jobPath = s.getOutputPath().substring(0, s.getOutputPath().lastIndexOf("/") + 1);
        String path = properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + jobPath;
        File temp = new File(path);
        temp.mkdirs();
        path = path + s.getID() + ".tmp";
        temp = new File(path);

        BufferedWriter writer = new BufferedWriter(new FileWriter(temp));

        writer.append(resultsBuilder);
        writer.flush();
        writer.close();

        List<String> command = new LinkedList<String>();
        command.add("python");
        command.add(properties.getProperty("gather_script"));
        command.add(properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + s.getOutputPath());
        command.add(path);
        command.add(String.valueOf(results.size()));
        command.add(String.valueOf(linesPerSample));
        command.add(String.valueOf(measurements.length + 1));

        System.out.print("Executing gather script...");

        Process shell = new ProcessBuilder(command).start();

        System.out.print("Exit code: " + shell.waitFor());

        String tempPath = results.get(0);
        tempPath = tempPath.substring(0, tempPath.lastIndexOf("/"));
        
        temp.delete();
        
        for (String r : results) {
            new File(r).delete();
        }
        
        new File(tempPath).delete();

        System.out.println("\nGatherer: Results saved to " + properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + s.getOutputPath());

        return s;
    }

    private String constructHeaders(String[] measurements, int samples) {
        StringBuilder headers = new StringBuilder();

        headers.append("# 0 - Iterations\n");

        int column = 1;
        for (int i = 0; i < measurements.length; i++) {
            for (int j = 0; j < samples; j++) {
                headers.append("# ");
                headers.append(column++);
                headers.append(" - ");
                headers.append(measurements[i]);
                headers.append(" (");
                headers.append(j);
                headers.append(")\n");
            }
        }

        return headers.toString();
    }

    private String[] getMeasurements(String result) {
        ArrayList<String> measurements = new ArrayList<String>();
        String m;

        Pattern p = Pattern.compile("#\\s[0-9]+\\s-.*\\s\\(");
        Matcher match = p.matcher(result);
        
        while (match.find()) {
            m = match.group();
            m = m.substring(m.indexOf("- ") + 2, m.length() - 2);

            measurements.add(m);
        }

        return measurements.toArray(new String[]{});
    }

}
