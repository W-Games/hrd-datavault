package com.hardrock.games.connectors;

import java.sql.SQLException;
import java.util.ArrayList;

import javax.sql.DataSource;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionPoolProvider {

    DataSource dataSource;
    private static ConnectionPoolProvider instance = null;
    private final static Object lock = new Object();

    public static ConnectionPoolProvider getInstance(String cloudSqlDb,
                                                     String cloudSqlInstanceConnectionName,
                                                     String cloudSqlUsername,
                                                     String cloudSqlPassword,
                                                     int minConnections,
                                                     int maxConnections) {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new ConnectionPoolProvider(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
                            cloudSqlPassword, minConnections, maxConnections);
                }
            }
        }
        return instance;
    }

    private ConnectionPoolProvider(
            String cloudSqlDb,
            String cloudSqlInstanceConnectionName,
            String cloudSqlUsername,
            String cloudSqlPassword,
            int minConnections,
            int maxConnections) {
        this.dataSource = getHikariDataSource(cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername,
                cloudSqlPassword, minConnections, maxConnections);
    }

    private DataSource getHikariDataSource(
            String cloudSqlDb,
            String cloudSqlInstanceConnectionName,
            String cloudSqlUsername,
            String cloudSqlPassword,
            int minConnections,
            int maxConnections) {
        HikariConfig config = new HikariConfig();
        // Configure which instance and what database user to connect with.
        config.setJdbcUrl("jdbc:postgresql://bi-prod-db-01.prod.wginfra.net:5432/" + cloudSqlDb); //jdbc:postgresql://host:port/database?
        config.setUsername(cloudSqlUsername);
        config.setPassword(cloudSqlPassword);
        config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
        config.addDataSourceProperty("cloudSqlInstance", cloudSqlInstanceConnectionName);

        config.addDataSourceProperty("useSSL", "false");
        // maximumPoolSize limits the total number of concurrent connections this pool
        // will keep.
        config.setMaximumPoolSize(maxConnections);
        // minimumIdle is the minimum number of idle connections Hikari maintains in the
        // pool.
        // Additional connections will be established to meet this value unless the pool
        // is full.
        config.setMinimumIdle(minConnections);
        // setConnectionTimeout is the maximum number of milliseconds to wait for a
        // connection checkout.
        // Any attempt to retrieve a connection from this pool that exceeds the set
        // limit will throw an
        // SQLException.
        config.setConnectionTimeout(10000); // 10 seconds
        // idleTimeout is the maximum amount of time a connection can sit in the pool.
        // Connections that
        // sit idle for this many milliseconds are retried if minimumIdle is exceeded.
        config.setIdleTimeout(600000); // 10 minutes
        // maxLifetime is the maximum possible lifetime of a connection in the pool.
        // Connections that
        // live longer than this many milliseconds will be closed and reestablished
        // between uses. This
        // value should be several minutes shorter than the database's timeout value to
        // avoid unexpected
        // terminations.
        config.setMaxLifetime(1800000); // 30 minutes

        return new HikariDataSource(config);
    }

    public DataSource getDataSource() {

        return dataSource;
    }

}

// And here
// my Transformer{code:java}

// @Override
// public PCollection<Void> expand(PCollection<Row> input) {

// return input
// .apply(ParDo.of(new DoFn<ValueInSingleWindow<Row>, Void>() {
// private List<ValueInSingleWindow<Row>> records = new ArrayList<>();
// private DataSource dataSource;

// @Setup
// public void setup(){
// dataSource=ConnectionPoolProvider.getInstance().getDataSource();
// }

// @ProcessElement
// public void process(ProcessContext context) {

// records.add(context.element());
// if (records.size() >= BATCH_SIZE) {
// executeBatch();
// }
// }

// @FinishBundle
// public void finishBundle() throws Exception {
// executeBatch();

// }

// private void executeBatch() {
// logger.debug("execute batch size"+records.size());
// if (records.isEmpty()) {
// return;
// }
// try(Connection connection = dataSource.getConnection();
// PreparedStatement preparedStatement = connection.prepareStatement("insert
// into SOME_TABLE values(??)"))
// {
// connection.setAutoCommit(true);
// for (ValueInSingleWindow<Row> record : records) {
// preparedStatement.clearParameters();
// prepareStatement(record, preparedStatement);
// preparedStatement.addBatch();
// }
// preparedStatement.executeBatch();

// } catch (SQLException e) {
// throw new RuntimeException("failed execute", e);
// }
// records.clear();
// }
// }));

// }