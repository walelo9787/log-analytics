package kafka;

import java.sql.*;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class Jdbc {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class.getName());
    private String jdbcUrl = "jdbc:mysql://localhost:3306/";
    private String userName;
    private String password = System.getenv("PASSWORD");
    private List<String> columns;
    private String table;
    private String dbName;
    private Set<Record> records;
    private Connection connection;

    public Jdbc(
        final Set<Record> records,
        final String user, 
        final String dbName,
        final String table,
        final List<String> columns) {
        this.userName = user;
        this.dbName = dbName;
        this.columns = columns;
        this.table = table;
        this.records = records;
        this.connection = getConnection();
    }

    private Connection getConnection() {
        LOGGER.info("Starting connection to MySql server.");;
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(jdbcUrl + dbName, userName, password);
            LOGGER.info("Connected to MySql server successfully : [database={}]", dbName);
        } catch (Exception e) {
            LOGGER.info("Connection to MySql server failed. Exception caught: ", e);
        }

        return conn;
    }

    private Statement executeQuery(final String query) {
        Statement stm = null;
        try {
            stm = connection.createStatement();
            LOGGER.info("Successfully created the statement.");
            stm.executeUpdate(query);
            LOGGER.info("Successfully executed\n [query={}]", query);
        } catch (Exception e) {
            LOGGER.info("Query execution failed.\n [query={}]\n [exception={}]", query, e);
        }
        
        return stm;

    }

    /**Creates table if it does not exist already.*/
    private void createTable() {
        String query = MessageFormat.format("CREATE TABLE IF NOT EXISTS {0} (\n", table)
            + "id INT AUTO_INCREMENT PRIMARY KEY,\n"
            + MessageFormat.format("{0} DATETIME,\n", columns.getFirst()) // date
            + MessageFormat.format("{0} VARCHAR(20),\n", columns.get(1)) // ip
            + MessageFormat.format("{0} VARCHAR(10),\n", columns.get(2)) // request
            + MessageFormat.format("{0} VARCHAR(50),\n", columns.get(3)) // endpoint
            + MessageFormat.format("{0} INT,\n", columns.get(4)) // status
            + MessageFormat.format("{0} VARCHAR(300),\n", columns.get(5)) // referrer
            + MessageFormat.format("{0} DECIMAL(10, 2),\n", columns.get(6)) // byte
            + MessageFormat.format("{0} VARCHAR(300),\n", columns.get(7)) // userAgent
            + MessageFormat.format("{0} DECIMAL(10, 2));", columns.get(8)); // responseTime
        
        executeQuery(query);
    }

    private void insertAll() {

        String cols = columns.stream()
            .collect(Collectors.joining(", "));

        String query = MessageFormat.format(
            "INSERT INTO {0} ({1}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", table, cols
            );

        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            
            for (Record record : records) {
                preparedStatement.setString(1, record.date);
                preparedStatement.setString(2, record.ip);
                preparedStatement.setString(3, record.request);
                preparedStatement.setString(4, record.endpoint);
                preparedStatement.setInt(5, record.status);
                preparedStatement.setString(6, record.referres);
                preparedStatement.setDouble(7, record.byte_);
                preparedStatement.setString(8, record.userAgent);
                preparedStatement.setDouble(9, record.responseTime);

                preparedStatement.addBatch();
            }

            // Execute the batch
            int[] rowsAffected = preparedStatement.executeBatch();
            LOGGER.info(rowsAffected.length + " row(s) inserted successfully.");
        } catch (Exception e) {
            LOGGER.info("Insertion failed. Exception caught : " + e);
        }
    }

    public void pushToDb() {
        createTable();
        insertAll();
    }

}
