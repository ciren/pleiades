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
import com.hazelcast.core.ITopic;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import net.pleiades.tasks.Task;
import net.pleiades.utility.PleiadesConfiguration;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class Config {
    private static Properties properties = null;
    private static final String configFile = "pleiades.conf";
            
    public static final String simulationsMap = "simulationsMap";
    public static final String runningMap = "runningMap";
    public static final String completedMap = "completedMap";
    public static final String fileMap = "fileMap";
    public static final String errorQueue = "errorQueue";
    public static final String distributor = "distributor";

    public static final Map<String, Task> RESEND_REQUEST = new HashMap<String, Task>();
    public static final int MaxJobsPerUser = Integer.MAX_VALUE;

    public static final ITopic TASKS_TOPIC = Hazelcast.getTopic("tasksTopic");
    public static final ITopic REQUESTS_TOPIC = Hazelcast.getTopic("requestTopic");
    public static final ITopic RESULTS_TOPIC = Hazelcast.getTopic("resultsTopic");
    public static final ITopic ERRORS_TOPIC = Hazelcast.getTopic("errorTopic");
    public static final ITopic CONTINUE_TOPIC = Hazelcast.getTopic("continueTopic");
    public static final ITopic HEARTBEAT_TOPIC = Hazelcast.getTopic("heartBeatTopic");

    public static Options createOptions() {
        Options o = new Options();
        Option help = new Option("h", "help", false, "Display Pleiades usage information");
        Option user = OptionBuilder.withArgName("username")
                .hasArg()
                .withDescription("Pleiades username")
                .withLongOpt("user")
                .create("u");
        Option file = OptionBuilder.withArgName("input file")
                .hasArg()
                .withDescription("Cilib input file")
                .withLongOpt("input")
                .create("i");
        
//        Option config = OptionBuilder.withArgName("config file")
//                .hasArg()
//                .withDescription("Pleiades configuration file (default: \"pleiades.conf\")")
//                .withLongOpt("config")
//                .create("c");

        Option worker = OptionBuilder.withArgName("worker count quietMode")
                .withDescription("Start Pleiades member in worker mode (ignores all other options)")
                .hasOptionalArg()
                .withLongOpt("worker")
                .create("w");
        
        Option jar = OptionBuilder.withArgName("jar file")
                .hasArg()
                .withDescription("Custom Cilib jar file")
                .withLongOpt("jar")
                .create("j");

        Option releaseType = OptionBuilder.withArgName("release type")
                .hasArg()
                .withDescription("Official Cilib jar release: options are 'official', 'custom' or 'master'.")
                .withLongOpt("release-type")
                .create("t");

        Option cont = OptionBuilder.withDescription("Attempts to continue a job that did not complete correctly")
                .withLongOpt("continue")
                .create();

        Option register = OptionBuilder.withDescription("Register a new user (ignores all other options)")
                .withLongOpt("register")
                .create("r");

        Option gatherer = OptionBuilder.withDescription("Start a Pleiades member in gatherer mode (ignores all other options)")
                .withLongOpt("gatherer")
                .create("g");

        Option monitor = OptionBuilder.withDescription("Start a Pleiades member in monitor mode (ignores all other options)")
                .withLongOpt("monitor")
                .create("m");

        Option verbose = OptionBuilder.withDescription("Hides the Pleiades text output to allow normal output stream to be displayed")
                .withLongOpt("quiet")
                .create("q");

        o.addOption(help);
        o.addOption(user);
        o.addOption(file);
        //o.addOption(config);
        o.addOption(worker);
        o.addOption(jar);
        o.addOption(releaseType);
        o.addOption(register);
        o.addOption(gatherer);
        o.addOption(monitor);
        o.addOption(verbose);
        o.addOption(cont);

        return o;
    }

    private static void loadConfiguration(String file) {

        Properties p = new Properties();

        try {
            p.load(new FileInputStream(file));
        } catch (IOException e) {
            System.out.print("Warning: Unable to load Pleiades configuration from file \""+ file + "\". Create new configuration? (yes/no): ");
            Scanner scanner = new Scanner(System.in);
            String input = scanner.next();

            if (input.equalsIgnoreCase("yes") || input.equalsIgnoreCase("y")) {
                p = PleiadesConfiguration.configure(file);
                System.out.println("Pleiades configuration complete.");
            } else {
                System.out.println("Error: No Pleiades configuration found.");
                System.exit(1);
            }
        }
        System.out.println("Configuration loaded successfully");
        
        properties = p;
    }

    public synchronized static Properties getConfiguration() {
        if (properties == null) {
            loadConfiguration(configFile);
        }
        
        return properties;
    }

}
