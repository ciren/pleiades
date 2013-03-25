/**
 * Pleiades
 * Copyright (C) 2011 - 2012
 * Computational Intelligence Research Group (CIRG@UP)
 * Department of Computer Science
 * University of Pretoria
 * South Africa
 */
package net.pleiades.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.pleiades.Config;
import net.pleiades.utility.SHA256;

/**
 *
 * @author bennie
 */
public class MySQLCommunicator implements UserDBCommunicator {
    Connection connection;

    public MySQLCommunicator() {
    }

    @Override
    public boolean connect() {
        Properties properties = Config.getConfiguration();
        
        String url = properties.getProperty("db_url");
        String user = properties.getProperty("db_user");
        String pass = properties.getProperty("db_pass");

        try {
            connection = DriverManager.getConnection("jdbc:mysql:" + url + "?autoReconnect=true", user, pass);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean userExists(String user) {
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT * FROM users WHERE username = '" + user + "'");

            if (results.next()) {
                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public boolean authenticateUser(String user) {
        try {
            String pass = new jline.ConsoleReader().readLine("Input password for " + user + ": ", new Character('*'));
            String hash = SHA256.getHashValue(pass);

            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT password FROM users WHERE username = '" + user + "'");

            if (results.next()) {
                pass = results.getString("password");
                
                if (pass.equals(hash.toString())) {
                    return true;
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return false;
    }

    @Override
    public boolean silentAuthenticateUser(String user, String password) {
        try {
            String hash = SHA256.getHashValue(password);

            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT password FROM users WHERE username = '" + user + "'");

            if (results.next()) {
                String pass = results.getString("password");

                if (pass.equals(hash.toString())) {
                    return true;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    @Override
    public String registerNewUser() {
        String user = null;
        
        try {
            user = new jline.ConsoleReader().readLine("Enter new username: ");

            while (userExists(user)) {
                user = new jline.ConsoleReader().readLine("User already exists. Enter new username: ");
            }

            String passw = new jline.ConsoleReader().readLine("Enter password: ", new Character('*'));
            String passr = new jline.ConsoleReader().readLine("Repeat password: ", new Character('*'));

            while (!passw.equals(passr)) {
                passw = new jline.ConsoleReader().readLine("Passwords did not match! Enter password: ", new Character('*'));
                passr = new jline.ConsoleReader().readLine("Repeat password: ", new Character('*'));
            }

            String email = new jline.ConsoleReader().readLine("Enter email address: ");

            Statement statement = connection.createStatement();
            StringBuilder query = new StringBuilder();

            query.append("INSERT INTO users values('");
            query.append(user); query.append("', '");
            query.append(email); query.append("', '");
            query.append(SHA256.getHashValue(passw));
            query.append("', 5);");

            statement.execute(query.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.out.println("Registration Successful.");

        return user;
    }

    @Override
    public String getUserEmail(String user) {
        try {
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT email FROM users WHERE username = '" + user + "'");

            if (results.next()) {
                return results.getString("email");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return "";
    }
}
