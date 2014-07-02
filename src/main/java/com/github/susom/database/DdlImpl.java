/*
 * Copyright 2014 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.susom.database;

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
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private static final Logger logQuiet = LoggerFactory.getLogger(Database.class.getName() + ".Quiet");
  private final Connection connection;
  private final String sql;
  private final Options options;

  public DdlImpl(Connection connection, String sql, Options options) {
    this.connection = connection;
    this.sql = sql;
    this.options = options;
  }

  private void updateInternal(boolean quiet) {
    CallableStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      ps = connection.prepareCall(sql);

      metric.checkpoint("prep");
      ps.execute();
      metric.checkpoint("exec");
      isSuccess = true;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw new DatabaseException(DebugSql.exceptionMessage(sql, null, errorCode, options), e);
    } finally {
      close(ps);
      metric.checkpoint("close");
      // PostgreSQL requires explicit commit since we are running with setAutoCommit(false)
      commit(connection);
      metric.done("commit");
      if (isSuccess) {
        DebugSql.logSuccess("DDL", log, metric, sql, null, options);
      } else if (quiet) {
        DebugSql.logWarning("DDL", logQuiet, metric, errorCode, sql, null, options, logEx);
      } else {
        DebugSql.logError("DDL", log, metric, errorCode, sql, null, options, logEx);
      }
    }
  }

  @Override
  public void execute() {
    updateInternal(false);
  }

  @Override
  public void executeQuietly() {
    try {
      updateInternal(true);
    } catch (DatabaseException e) {
      // Ignore, as requested
    }
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

  private void commit(Connection c) {
    if (c != null) {
      try {
        c.commit();
      } catch (Exception e) {
        log.warn("Caught exception on commit", e);
      }
    }
  }
}
