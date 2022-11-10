/*******************************************************************************
 * Copyright 2014 Observational Health Data Sciences and Informatics
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package ohdsi.databases;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector {

    private DBConnector() {
    }

    public static Connection connect(String server, String user, String password, DbType dbType) {
        return switch (dbType) {
            case MSSQL -> DBConnector.connectToMSSQL(server, user, password);
            case MYSQL -> DBConnector.connectToMySQL(server, user, password);
            case POSTGRESQL -> DBConnector.connectToPostgreSQL(server, user, password);
        };
    }

    public static Connection connectToPostgreSQL(String server, String user, String password) {
        if (!server.contains("/"))
            throw new RuntimeException("For PostgreSQL, database name must be specified in the server field (<host>/<database>)");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e1) {
            throw new RuntimeException("Cannot find JDBC driver. Make sure the file postgresql-x.x-xxxx.jdbcx.jar is in the path");
        }
        String url = "jdbc:postgresql://" + server;
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e1) {
            throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
        }
    }

    public static Connection connectToMySQL(String server, String user, String password) {

        String url = "jdbc:mysql://" + server + ":3306/?useCursorFetch=true&allowPublicKeyRetrieval=true&useSSL=false&verifyServerCertificate=false";

        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e1) {
            throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
        }
    }

    public static Connection connectToMSSQL(String server, String user, String password) {
        if (user == null || user.length() == 0) { // Use Windows integrated security
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } catch (ClassNotFoundException e1) {
                throw new RuntimeException("Cannot find JDBC driver. Make sure the file sqljdbc4.jar is in the path");
            }
            String url = "jdbc:sqlserver://" + server + ";integratedSecurity=true";

            try {
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e1) {
                throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
            }
        } else { // Do not use Windows integrated security
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } catch (ClassNotFoundException e1) {
                throw new RuntimeException("Cannot find JDBC driver. Make sure the file jtds-1.3.0.jar is in the path");
            }
            String url = "jdbc:sqlserver://" + server;
            try {
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e1) {
                throw new RuntimeException("Cannot connect to DB server: " + e1.getMessage());
            }
        }

    }
}
