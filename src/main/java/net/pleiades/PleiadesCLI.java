/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import com.google.common.base.Preconditions;
import com.hazelcast.core.Hazelcast;
import java.util.Arrays;
import net.pleiades.cluster.HazelcastCommunicator;
import net.pleiades.database.UserDBCommunicator;
import org.apache.commons.cli.*;

/**
 *
 * @author bennie
 */
public class PleiadesCLI {
    static final String VERSION = "0.2";
    static final int MIN_MEMBERS = 2;

    public static void main(String args[]) {
        CommandLineParser parser = new GnuParser();

        try {
            Options options = Config.createOptions();
            CommandLine cli = parser.parse(options, args);
            handleCommandline(options, cli, args);
        } catch (ParseException e) {
            System.err.println(">Error: Failed to parse command line arguments. Use --help to see options.\n" + e.getMessage());
            System.exit(1);
        }
    }

    private static void user(CommandLine cli, String user) {
        HazelcastCommunicator cluster = new HazelcastCommunicator();
        //UserDBCommunicator database = Utils.authenticate(properties, user);
        UserDBCommunicator database = Utils.connectToDatabase();

        cluster.connect();
        int clusterSize = Hazelcast.getCluster().getMembers().size();

        if (clusterSize < MIN_MEMBERS) {
            System.out.println(">Error: Too few cluster members active.>Connection terminated. If problem persists, contact cluster administrator.");
            System.exit(0);
        }

        System.out.println(">>Now connected to Pleiades Cluster (" + clusterSize +" members).>You are logged in as " + user + ".>");

        if (cli.hasOption("input") && cli.hasOption("release-type") && cli.hasOption("jar")) {

            String input = cli.getOptionValue("input");
            String jar = cli.getOptionValue("jar");
            String releaseType = cli.getOptionValue("release-type");
            //boolean cont = cli.hasOption("continue"); //for the future

            if (!Arrays.asList("master", "official", "custom").contains(releaseType.toLowerCase())) {
                System.out.println(">Error: Valid arguments for 'release-type' are 'official', 'master' and 'custom'.");
                System.exit(1);
            }

            if (jar == null) {
                System.out.println(">Error: Option 'jar' takes an argument.");
                System.exit(1);
            }

            User.uploadJob(input, jar, user, database.getUserEmail(user), releaseType);
        } else {
            System.out.println(">Error: Options 'jar', 'input' and 'release-type' are required with option 'user'.");
            System.exit(1);
        }
    }

    private static void continueJob(CommandLine cli) {
        Config.CONTINUE_TOPIC.publish(cli.getOptionValue("continue"));
    }

    private static void handleCommandline(Options options, CommandLine cli, String args[]) {
        Preconditions.checkState(args.length > 0, ">Error: You must either specify a user or start Pleiades member in worker mode! Use --help for more options.");

        if (cli.hasOption("help")) {
            new HelpFormatter().printHelp("Pleiades", options);
            System.exit(0);
        }

        if (cli.hasOption("worker")) {
            //Preconditions.checkState(cli.getOptions().length == 1, "Option --worker must be used without any other options.");
            System.out.print("Starting worker pool");
            new WorkerPool(Integer.parseInt(cli.getOptionValue("worker", "1")), cli.hasOption("quiet")).execute();
        } else if (cli.hasOption("register")) {
            Preconditions.checkState(cli.getOptions().length == 1, "Option --register must be used without any other options.");
            Utils.connectToDatabase().registerNewUser();
            System.out.println("Thank you for registering on Pleiades! You may now log in with your new username.");
        } else if (cli.hasOption("gatherer")) {
            Preconditions.checkState(cli.getOptions().length < 3, "Too many options");
            if (cli.getOptions().length == 2) {
                Preconditions.checkState(cli.hasOption("continue"), "Gatherer can only be used with option --continue");
            }
            new Gatherer().start(cli.hasOption("continue"));
        } else if (cli.hasOption("user")) {
            String user = cli.getOptionValue("user");
            user(cli, user);
        } else {
            System.out.println(">Error: You must specify a user or start Pleiades in worker mode! Use --help for more options.");
        }
    }
}
