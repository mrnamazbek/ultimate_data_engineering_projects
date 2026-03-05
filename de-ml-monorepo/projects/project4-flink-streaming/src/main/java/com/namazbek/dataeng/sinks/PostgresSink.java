package com.namazbek.dataeng.sinks;

import com.namazbek.dataeng.models.EventMetrics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * PostgreSQL sink for Flink streaming results
 * Uses JDBC connection with connection pooling and retry logic
 */
public class PostgresSink extends RichSinkFunction<EventMetrics> {
    
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSink.class);
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;
    
    private Connection connection;
    private PreparedStatement insertStatement;

    public PostgresSink(String jdbcUrl, String username, String password, String tableName) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        establishConnection();
        createTableIfNotExists();
        prepareInsertStatement();
        LOG.info("PostgresSink initialized for table: {}", tableName);
    }

    private void establishConnection() throws SQLException {
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(true);
            LOG.info("Connected to PostgreSQL: {}", jdbcUrl);
        } catch (ClassNotFoundException e) {
            throw new SQLException("PostgreSQL JDBC driver not found", e);
        }
    }

    private void createTableIfNotExists() throws SQLException {
        String createTableSQL = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                metric_type VARCHAR(100) NOT NULL,
                event_count BIGINT DEFAULT 0,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                avg_processing_time DOUBLE PRECISION,
                additional_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_user_id (user_id),
                INDEX idx_metric_type (metric_type),
                INDEX idx_window_start (window_start)
            )
        """, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(createTableSQL)) {
            stmt.execute();
            LOG.info("Table {} created or verified", tableName);
        }
    }

    private void prepareInsertStatement() throws SQLException {
        String insertSQL = String.format("""
            INSERT INTO %s (user_id, metric_type, event_count, window_start, window_end, 
                           avg_processing_time, additional_data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, tableName);
        
        insertStatement = connection.prepareStatement(insertSQL);
    }

    @Override
    public void invoke(EventMetrics metrics, Context context) throws Exception {
        int retryCount = 0;
        int maxRetries = 3;
        
        while (retryCount < maxRetries) {
            try {
                // Reconnect if connection is closed
                if (connection.isClosed()) {
                    LOG.warn("Connection closed, reconnecting...");
                    establishConnection();
                    prepareInsertStatement();
                }

                // Set parameters
                insertStatement.setObject(1, metrics.getUserId());
                insertStatement.setString(2, metrics.getMetricType());
                insertStatement.setLong(3, metrics.getEventCount() != null ? metrics.getEventCount() : 0L);
                insertStatement.setTimestamp(4, metrics.getWindowStart());
                insertStatement.setTimestamp(5, metrics.getWindowEnd());
                insertStatement.setObject(6, metrics.getAvgProcessingTime());
                insertStatement.setString(7, metrics.getAdditionalData());

                // Execute insert
                insertStatement.executeUpdate();
                
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Inserted metrics: {}", metrics);
                }
                
                break; // Success, exit retry loop
                
            } catch (SQLException e) {
                retryCount++;
                LOG.error("Failed to insert metrics (attempt {}/{}): {}", retryCount, maxRetries, e.getMessage());
                
                if (retryCount >= maxRetries) {
                    LOG.error("Max retries reached, dropping metric: {}", metrics);
                    break;
                } else {
                    // Wait before retry
                    Thread.sleep(1000 * retryCount);
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        
        if (insertStatement != null) {
            insertStatement.close();
        }
        
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("PostgreSQL connection closed");
        }
    }
}
