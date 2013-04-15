/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.results;

import com.google.common.io.Files;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.pleiades.Config;
import net.pleiades.simulations.Simulation;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author bennie
 */
public class CilibSampleGatherer implements SampleGatherer {

    private Properties properties;

    public CilibSampleGatherer() {
        this.properties = Config.getConfiguration();
    }

    @Override
    public synchronized Simulation gatherResults(IMap<String, Simulation> completedMap, Simulation s) throws Throwable {
        completedMap.remove(s.getID());

        List<String> results = s.getResults();
        StringBuilder resultsBuilder = new StringBuilder();
        String[] measurements = new String[]{};
        boolean gotHeaders = false;
        int linesPerSample = 0;

        for (String r : results) {
            StringBuilder thisResult = new StringBuilder();
            BufferedReader reader = new BufferedReader(new FileReader(new File(r)));

            while (reader.ready()) {
                thisResult.append(reader.readLine());
                thisResult.append("\n");
            }

            String thisResultString = thisResult.toString();

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

        if (s.getReleaseType().equals("official") || s.getReleaseType().equals("master")) {
            pushResults(properties.getProperty("gather_results_folder") + s.getOwner() + "/" + jobName + "/" + s.getOutputPath(),
                        s.getCilibInput(), s.getFileKey(), s.getOwner());
        }

        return s;
    }

    private boolean pushResults(String resultsFile, String spec, String jarFilename, String user) {
        try {
            String id = Long.toString(System.currentTimeMillis());
            IMap<String, byte[]> jars = Hazelcast.getMap(Config.fileMap);
            byte[] jarBytes = jars.get(jarFilename);
            File jarFile = File.createTempFile("jar" + id + "-", ".jar");
            Files.write(jarBytes, jarFile);
            jars.forceUnlock(jarFilename);

            File specFile = File.createTempFile("spec" + id + "-", ".xml");
            Files.write(spec, specFile, Charset.defaultCharset());

            System.out.println("Submitting results to ciDB");
            System.out.println("jar: " + jarFile.getAbsolutePath());
            System.out.println("spec: " + specFile.getAbsolutePath());
            System.out.println("results: " + resultsFile);

            List<String> command = new LinkedList<String>();
            StringTokenizer tokens = new StringTokenizer(properties.getProperty("cidb_submit_command"));
            while (tokens.hasMoreTokens()) {
                command.add(tokens.nextToken()
                        .replaceAll("\\$cidb_jar", properties.getProperty("cidb_jar"))
                        .replaceAll("\\$cidb_conf", properties.getProperty("cidb_conf"))
                        .replaceAll("\\$jar", jarFile.getAbsolutePath())
                        .replaceAll("\\$spec", specFile.getAbsolutePath())
                        .replaceAll("\\$results", resultsFile)
                        .replaceAll("\\$user", user));
            }

            Process process = new ProcessBuilder(command).start();

            System.out.println("Submission complete... Exit code: " + process.waitFor());

            jarFile.delete();
            specFile.delete();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: Could not submit results to database.");
        }
        return false;
    }

    private String constructHeaders(String[] measurements, int samples) {
        StringBuilder headers = new StringBuilder("# 0 - Iterations\n");

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
