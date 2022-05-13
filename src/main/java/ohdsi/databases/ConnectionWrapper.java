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

import ohdsi.medlineXmlToDatabase.Abbreviator;
import ohdsi.medlineXmlToDatabase.MedlineCitationAnalyser.VariableType;
import ohdsi.utilities.StringUtilities;
import ohdsi.utilities.files.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ohdsi.databases.DbType.MSSQL;
import static ohdsi.databases.DbType.MYSQL;
import static ohdsi.databases.DbType.POSTGRESQL;

/**
 * Wrapper around java.sql.connection to handle any database work that is platform-specific.
 *
 * @author MSCHUEMI
 */
public class ConnectionWrapper implements AutoCloseable{

    private static final Logger log = LogManager.getLogger(ConnectionWrapper.class.getName());
    private final Connection connection;
    private final DbType dbType;
    private boolean batchMode = false;
    private Statement statement;

    public ConnectionWrapper(String server, String user, String password, DbType dbType) {
        this.connection = DBConnector.connect(server, user, password, dbType);
        this.dbType = dbType;
        log.debug("Connected to {} database", this.dbType);
    }

    public void setBatchMode(boolean batchMode) {
        try {
            if (this.batchMode && !batchMode) { // turn off batchmode
                this.batchMode = false;
                statement.executeBatch();
                statement.close();
                connection.setAutoCommit(true);
            } else {
                this.batchMode = true;
                connection.setAutoCommit(false);
                statement = connection.createStatement();
            }
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            e = e.getNextException();
            if (e != null) {
                System.err.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
            throw new RuntimeException("Error executing batch data");
        }
    }

    /**
     * Switch the database to use.
     *
     * @param database
     */
    public void use(String database) {
        if (dbType.equals(POSTGRESQL))
            execute("SET search_path TO " + database);
        else
            execute("USE " + database);
    }

    public void createDatabase(String database) {
        execute("DROP SCHEMA IF EXISTS " + database + " CASCADE");
        execute("CREATE SCHEMA " + database);
    }

    /**
     * Execute the given SQL statement.
     *
     * @param sql
     */
    public void execute(String sql) {
        try {
            if (sql.length() == 0)
                return;
            if (batchMode)
                statement.addBatch(sql);
            else {
                try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    stmt.execute(sql);
                }
            }
        } catch (SQLException e) {
            System.err.println(sql);
            e.printStackTrace();
            SQLException nextException = e.getNextException();
            if (nextException != null) {
                System.err.println("Error: " + nextException.getMessage());
                nextException.printStackTrace();
            }
            throw new RuntimeException("Error inserting data");
        }
    }

    public void insertIntoTable(String table, Map<String, String> field2Value) {
        List<String> fields = new ArrayList<>(field2Value.keySet());

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");
        sql.append(Abbreviator.abbreviate(table));
        sql.append(" (");
        boolean first = true;
        for (String field : fields) {
            if (first)
                first = false;
            else
                sql.append(",");
            sql.append(Abbreviator.abbreviate(field));
        }

        if (dbType.equals(MYSQL)) { // MySQL uses double quotes, escape using backslash
        sql.append(") VALUES (\"");
        first = true;
        for (String field : fields) {
            if (first)
                first = false;
            else
                sql.append("\",\"");
            sql.append(field2Value.get(field).replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\""));
        }
        sql.append("\");");
        } else if (dbType.equals(MSSQL) || dbType.equals(POSTGRESQL)) { // MSSQL uses single quotes, escape by doubling
            sql.append(") VALUES ('");
            first = true;
            for (String field : fields) {
                if (first)
                    first = false;
                else
                    sql.append("','");
                sql.append(field2Value.get(field).replace("'", "''"));
            }
            sql.append("')");
        }
        execute(sql.toString());
    }

    public void insertIntoTable(String tableName, List<Row> rows, boolean emptyStringToNull) {
        List<String> columns = rows.get(0).getFieldNames();
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName);
        sql.append(" (").append(StringUtilities.join(columns, ",")).append(")");
        sql.append(" VALUES (?");
        sql.append(",?".repeat(Math.max(0, columns.size() - 1)));
        sql.append(")");
        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())){
            connection.setAutoCommit(false);

            for (Row row : rows) {
                for (int i = 0; i < columns.size(); i++) {
                    String value = row.get(columns.get(i));
                    if (value == null)
                        System.out.println(row);
                    if (value.length() == 0 && emptyStringToNull)
                        value = null;
                    if (dbType.equals(POSTGRESQL)) // PostgreSQL does not allow unspecified types
                        stmt.setObject(i + 1, value, Types.OTHER);
                    else
                        stmt.setString(i + 1, value);
                }
                stmt.addBatch();
            }
            stmt.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
            connection.clearWarnings();
        } catch (SQLException e) {
            e.printStackTrace();
            if (e instanceof BatchUpdateException) {
                System.err.println(e.getNextException().getMessage());
            }
        }
    }

    public void createTable(String table, List<String> fields, List<String> types, List<String> primaryKey) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(table).append(" (\n");
        boolean first = true;
        for (int i = 0; i < fields.size(); i++) {
            if (first)
                first = false;
            else
                sql.append(",\n");
            sql.append("  ").append(fields.get(i)).append(" ").append(types.get(i));
        }
        if (primaryKey != null && !primaryKey.isEmpty())
            sql.append(",\n  PRIMARY KEY (").append(StringUtilities.join(primaryKey, ",")).append(")\n");
        sql.append(");\n\n");
        execute(Abbreviator.abbreviate(sql.toString()));
    }

    public void createTableUsingVariableTypes(String table, List<String> fields, List<VariableType> variableTypes, List<String> primaryKey) {
        List<String> types = new ArrayList<String>(variableTypes.size());
        for (VariableType variableType : variableTypes) {
            if (dbType.equals(MYSQL)) {
                if (variableType.isNumeric)
                    types.add("INT");
                else if (variableType.maxLength > 255)
                    types.add("TEXT");
                else
                    types.add("VARCHAR(255)");
            } else if (dbType.equals(MSSQL)) {
                if (variableType.isNumeric) {
                    if (variableType.maxLength < 10)
                        types.add("INT");
                    else
                        types.add("BIGINT");
                } else if (variableType.maxLength > 255)
                    types.add("VARCHAR(MAX)");
                else
                    types.add("VARCHAR(255)");
            } else if (dbType.equals(POSTGRESQL)) {
                if (variableType.isNumeric) {
                    if (variableType.maxLength < 10)
                        types.add("INT");
                    else
                        types.add("BIGINT");
                } else if (variableType.maxLength > 255)
                    types.add("TEXT");
                else
                    types.add("VARCHAR(255)");
            } else
                throw new RuntimeException("Unknown datasource type " + dbType);
        }

        createTable(table, fields, types, primaryKey);
    }

    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean existsForPMIDAndVersion(String pmid, String pmidVersion) {
     return existsForPMIDAndVersion(pmid, pmidVersion, null);
    }

    public boolean existsForPMIDAndVersion(String pmid, String pmidVersion, String table) {
        String target = table == null ? "medcit" : table;
            try (PreparedStatement ps = connection.prepareStatement("SELECT pmid FROM " + target + " WHERE pmid = ? AND pmid_version = ? LIMIT 1")) {
                ps.setString(1, pmid);
                ps.setString(2, pmidVersion);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        return false;
    }

    public void deleteAllForPMIDAndVersion(Set<String> tables, String pmid, String pmidVersion) {
        try (Statement stmt = connection.createStatement()) {
            for (String table : tables) {
                String sql = "DELETE FROM " + Abbreviator.abbreviate(table) + " WHERE pmid = " + pmid + " AND pmid_version = " + pmidVersion;
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }


    }

    public class QueryResult implements Iterable<Row> {
        private String sql;

        private List<DBRowIterator> iterators = new ArrayList<DBRowIterator>();

        public QueryResult(String sql) {
            this.sql = sql;
        }

        @Override
        public Iterator<Row> iterator() {
            DBRowIterator iterator = new DBRowIterator(sql);
            iterators.add(iterator);
            return iterator;
        }

    }

    public List<String> getTableNames(String database) {
        List<String> names = new ArrayList<String>();
        String query = null;
        if (dbType.equals(MYSQL)) {
            if (database == null)
                query = "SHOW TABLES";
            else
                query = "SHOW TABLES IN " + database;
        } else if (dbType.equals(MSSQL)) {
            query = "SELECT name FROM " + database + ".sys.tables ";
        } else if (dbType.equals(POSTGRESQL)) {
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + database + "'";
        }
        for (Row row : query(query))
            names.add(row.get(row.getFieldNames().get(0)));
        return names;
    }

    public List<FieldInfo> getFieldInfo(String table) {
        List<FieldInfo> fieldInfos = new ArrayList<FieldInfo>();
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns(null, null, table, null);
            while (resultSet.next()) {
                FieldInfo fieldInfo = new FieldInfo();
                fieldInfo.name = resultSet.getString("COLUMN_NAME");
                fieldInfo.type = resultSet.getInt("DATA_TYPE");
                fieldInfo.length = resultSet.getInt("COLUMN_SIZE");
                fieldInfos.add(fieldInfo);
            }
        } catch (SQLException e) {
            throw (new RuntimeException(e));
        }
        return fieldInfos;
    }

    public class FieldInfo {
        public int type;
        public String name;
        public int length;
    }

    public QueryResult query(String sql) {
        return new QueryResult(sql);
    }

    private class DBRowIterator implements Iterator<Row>,AutoCloseable {

        private ResultSet resultSet;

        private boolean hasNext;

        private Set<String> columnNames = new HashSet<String>();

        public DBRowIterator(String sql) {
                sql.trim();
                if (sql.endsWith(";"))
                    sql = sql.substring(0, sql.length() - 1);
           try {
               Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
               resultSet = statement.executeQuery(sql);
               hasNext = resultSet.next();
            } catch (SQLException e) {
                System.err.println(sql);
                System.err.println(e.getMessage());
                throw new RuntimeException(e);
            }
        }

        public void close() {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                resultSet = null;
                hasNext = false;
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Row next() {
            try {
                Row row = new Row();
                ResultSetMetaData metaData;
                metaData = resultSet.getMetaData();
                columnNames.clear();

                for (int i = 1; i < metaData.getColumnCount() + 1; i++) {
                    String columnName = metaData.getColumnName(i);
                    if (columnNames.add(columnName)) {
                        String value = resultSet.getString(i);
                        if (value == null)
                            value = "";

                        row.add(columnName, value.replace(" 00:00:00", ""));
                    }
                }
                hasNext = resultSet.next();
                if (!hasNext) {
                    resultSet.close();
                    resultSet = null;
                }
                return row;
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
        }
    }

    public void setDateFormat() {
        if (!dbType.equals(MYSQL)) {
            try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)){
                if (dbType.equals(POSTGRESQL)) {
                    stmt.execute("SET datestyle = \"ISO, MDY\"");
                } else {
                    stmt.execute("SET dateformat DMY;");
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void dropTableIfExists(String table) {
        if (dbType.equals(MYSQL)) {
            execute("DROP TABLE IF EXISTS " + table);
        }
        else if (dbType.equals(POSTGRESQL)) {
            try (Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)){
                stmt.execute("TRUNCATE TABLE " + table);
                stmt.execute("DROP TABLE " + table);
            } catch (Exception e) {
                // do nothing
            }
        } else if (dbType.equals(MSSQL)) {
            execute("IF OBJECT_ID('" + table + "', 'U') IS NOT NULL DROP TABLE " + table + ";");
        } else {
            throw new RuntimeException("Could not execute statement, unknown dbType " + dbType);
        }
    }
}
