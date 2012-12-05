/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Properties;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import net.pleiades.database.UserDBCommunicator;
import net.pleiades.database.MySQLCommunicator;
import net.pleiades.simulations.Simulation;

public class Utils {
    
    public static UserDBCommunicator connectToDatabase(Properties p) {
        UserDBCommunicator db = new MySQLCommunicator(p);

        if(!db.connect()) {
            System.out.println("Error: Unable to connect to database.");
            System.exit(1);
        }

        return db;
    }

    public static SmbFileInputStream getJarFile(String jarPath) {
        SmbFileInputStream in = null;
        SmbFile jar = null;

        try {
            NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(null, "CiClops", "ciclops5813x");
            jar = new SmbFile(jarPath, auth);
            in = new SmbFileInputStream(jar);
        } catch (SmbException e) {
            System.out.println("Error: Unable to locate jar file!\n" + e.getMessage());
            System.exit(1);
        } catch (UnknownHostException e) {
            System.out.println("Error: Host not found when trying to get jar file!\n" + e.getMessage());
            System.exit(1);
        } catch (MalformedURLException e) {
            System.out.println("Error: Malformed URL!\n" + e.getMessage());
            System.exit(1);
        }

        return in;
    }

    public static void emailUser(Simulation simulation, File messageTemplate, Properties p, String extra) {
        String owner = simulation.getOwner();
        String email = simulation.getOwnerEmail();
        String link = "link";
        StringBuilder message = new StringBuilder();
        System.out.print("Sending email to " + email);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(messageTemplate));
            System.out.print("*\n" + extra + "\n");
            while (reader.ready()) {
                String m = reader.readLine();
                m = m.replace("{user}", owner);
                m = m.replace("{link}", link);
                m = m.replace("{job}", simulation.getJobName());
                m = m.replace("{sim_num}", String.valueOf(simulation.getSimulationNumber()));
                m = m.replace("{extra}", extra);
                        
                message.append(m);
                message.append("\n");
            }
            System.out.print(p.getProperty("email_script"));
            
            Process shell = new ProcessBuilder("python", p.getProperty("email_script"), email, message.toString()).start();
            System.out.println(shell.waitFor());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void emailAdmin(String message, Properties p) {
        try {
            Process shell = new ProcessBuilder("python", p.getProperty("email_script"), p.getProperty("admin_mail"), message).start();
            System.out.println(shell.waitFor());
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public static UserDBCommunicator authenticate(Properties properties, String user) {
        UserDBCommunicator database = Utils.connectToDatabase(properties);
        int guesses = 3;

        while (!database.authenticateUser(user)) {
            sleep(1000);
            System.out.println("Authentication failed!");
            guesses--;

            if (guesses == 0) {
                System.exit(1);
            }
        }

        return database;
    }

    public static boolean silentAuthenticate(Properties properties, String user, String password) {
        UserDBCommunicator database = Utils.connectToDatabase(properties);

        return database.silentAuthenticateUser(user, password);
    }

    public static void sleep(int time) {
        try {
            Thread.sleep(time);
        } catch(InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    public static String getSocketStringFromWorkerID(String id) {
        return id.substring(0, id.lastIndexOf("-"));
    }

    public static String header =
            "  _____  _      _           _          \n" +
            " |  __ \\| |    (_)         | |          \n" +
            " | |__) | | ___ _  __ _  __| | ___ ___ \n" +
            " |  ___/| |/ _ \\ |/ _` |/ _` |/ _ | __| * Cluster *\n" +
            " | |    | |  __/ | (_| | (_| |  __|__ \\\n" +
            " |_|    |_|\\___|_|\\__,_|\\__,_|\\___|___/\n";
}
