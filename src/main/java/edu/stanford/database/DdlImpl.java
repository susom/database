package edu.stanford.database;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class DdlImpl implements Ddl {
  private static final Logger log = LoggerFactory.getLogger(DdlImpl.class);
  private final Connection connection;
  private final String sql;

  public DdlImpl(Connection connection, String sql) {
    this.connection = connection;
    this.sql = sql;
  }

  private void updateInternal() {
    CallableStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    try {
      ps = connection.prepareCall(sql);

      metric.checkpoint("prep");
      ps.execute();
      metric.checkpoint("exec");
    } catch (Exception e) {
      throw new DatabaseException(toMessage(sql), e);
    } finally {
      close(ps);
      metric.done("close");
      if (log.isDebugEnabled()) {
        log.debug("Update: " + metric.getMessage() + " " + new DebugSql(sql, null));
      }
    }
  }

  @Override
  public void execute() {
    updateInternal();
  }

  @Override
  public void executeQuietly() {
    try {
      updateInternal();
    } catch (DatabaseException e) {
      // Ignore, as requested
    }
  }

  private String toMessage(String sql) {
    return "Error executing SQL: " + new DebugSql(sql, null);
  }

  private void close(Statement s) {
    if (s != null) {
      try {
        s.close();
      } catch (Exception e) {
        log.warn("Caught exception closing the Statement", e);
      }
    }
  }
}
