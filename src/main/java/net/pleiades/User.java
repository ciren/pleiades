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
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Transaction;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.locks.Lock;
import jcifs.smb.SmbFileInputStream;
import net.pleiades.simulations.Simulation;
import net.pleiades.simulations.creator.SimulationsCreator;
import net.pleiades.simulations.creator.XMLSimulationsCreator;
import net.pleiades.tasks.Task;

public class User {
    public static void uploadJob(Properties properties, String input, String jar, String user, String userEmail) {
        File run = null;
        FileInputStream runInputStream = null;
        byte[] runBytes = null;
        //System.out.print("1");
        try {
            if (jar != null) {
                System.out.println(">Info: Using custom jar file.>");
                run = new File(jar);
            } else {
                System.out.println(">Info: Using default jar file.>");
                SmbFileInputStream smbJar = Utils.getJarFile(properties.getProperty("ci_jar"));

                run = new File("pleiades.run");
                run.deleteOnExit();
                FileOutputStream fout = new FileOutputStream(run);
                byte[] b = new byte[8192];
                int n;

                while ((n = smbJar.read(b)) > 0) {
                    fout.write(b, 0, n);
                }
                fout.close();
            }
            //System.out.print("2");
            runInputStream = new FileInputStream(run);
            runBytes = new byte[runInputStream.available()];
            runInputStream.read(runBytes);
            //System.out.print("3");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.out.print("4");
        if (run == null) {
            System.out.println("Error: jar file not specified! Check your Pleiades configuration or use --jar to specify a custom jar file.");
            System.exit(1);
        }
        //System.out.print("5");
        //System.out.print(">Aquiring lock...");
        Lock jLock = Hazelcast.getLock(Config.simulationsMap);
        Lock fLock = Hazelcast.getLock(Config.fileMap);
        Lock cLock = Hazelcast.getLock(Config.completedMap);
        //System.out.print("6");
        jLock.lock();
        //System.out.print("7");
        fLock.lock();
        //System.out.print("8");
        cLock.lock();
        //System.out.print("9");

        File inputFile = new File(input);
        String absPath = inputFile.getAbsolutePath();
        String jobName = absPath.substring(absPath.replaceAll("\\\\", "/").lastIndexOf("/") + 1, absPath.lastIndexOf("."));
        String fileKey = user + "_" + jobName;
        //System.out.print("10");
        System.out.println(">Creating job from file (" + input + ")>");
        System.out.println("Job name set to: " + jobName + ">");
        SimulationsCreator creator = new XMLSimulationsCreator(user, userEmail, user, jobName);
        List<Simulation> simulations = creator.createSimulations(inputFile, fileKey);//runBytes);
        ITopic tasksTopic = Hazelcast.getTopic(Config.tasksTopic);

        System.out.print("Your job contains " + simulations.size() + " simulations.>");

        IMap<String, List<Simulation>> simulationsMap = Hazelcast.getMap(Config.simulationsMap);
        IMap<String, byte[]> fileQueue = Hazelcast.getMap(Config.fileMap);
        IMap<String, Simulation> completedMap = Hazelcast.getMap(Config.completedMap);

        Transaction txn = Hazelcast.getTransaction();
        txn.begin();

        int tasks = 0;
        try {

            System.out.print(">Uploading tasks to cluster...");
            fileQueue.put(fileKey, runBytes);
            List<Simulation> jobsList = simulationsMap.remove(user);
            if (jobsList == null) {
                jobsList = new LinkedList<Simulation>();
            }
            jobsList = new LinkedList<Simulation>(jobsList);
            
            int len = 0;
            double sim = 0;
            //DecimalFormat df = new DecimalFormat("#.##");
            //StringBuilder progress;
            for (Simulation s : simulations) {
//                progress = new StringBuilder();
//                for (int i = 0; i < len; i++) {
//                    System.out.print("\b");
//                }
//                double p = (sim / simulations.size()) * 100;
//                progress.append(df.format(p));
//                progress.append("%");
//                System.out.print(progress);
//                len = progress.length();
//                sim++;
                
                //s.generateTasks();
                tasks += s.getSamples();
                jobsList.add(s);
                completedMap.put(s.getID(), s.emptyClone());
                completedMap.forceUnlock(s.getID());
            }
            
            simulationsMap.put(user, jobsList);
            
//            String finalizing = " finalizing... (this may take some time)";
//            len += finalizing.length();
//            System.out.print(finalizing);
            
            txn.commit();
            
//            for (int i = 0; i < len; i++) {
//                System.out.print("\b");
//            }
            System.out.print("100%>");
//            for (int i = 0; i < finalizing.length(); i++) {
//                System.out.print(" ");
//            }
            
        } catch (Throwable e) {
            e.printStackTrace();
            txn.rollback();
        } finally {
            simulationsMap.forceUnlock(user);
            fileQueue.forceUnlock(fileKey);
            cLock.unlock();
            fLock.unlock();
            jLock.unlock();
        }

        tasksTopic.publish(new HashMap<String, Task>());
        
        System.out.println(tasks + " tasks uploaded.>");
        System.out.println(">Your job was created successfully.>You will be notified via email (" + userEmail + ") when your results are available.>");

        Hazelcast.getLifecycleService().shutdown();
        System.exit(0);
    }
}
