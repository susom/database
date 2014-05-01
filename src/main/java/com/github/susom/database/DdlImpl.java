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
  private static final Logger log = LoggerFactory.getLogger(DdlImpl.class);
  private final Connection connection;
  private final String sql;
  private final LogOptions logOptions;

  public DdlImpl(Connection connection, String sql, LogOptions logOptions) {
    this.connection = connection;
    this.sql = sql;
    this.logOptions = logOptions;
  }

  private void updateInternal() {
    CallableStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    boolean isSuccess = false;
    String errorCode = null;
    try {
      ps = connection.prepareCall(sql);

      metric.checkpoint("prep");
      ps.execute();
      metric.checkpoint("exec");
      isSuccess = true;
    } catch (Exception e) {
      errorCode = logOptions.generateErrorCode();
      throw new DatabaseException(DebugSql.exceptionMessage(sql, null, errorCode, logOptions), e);
    } finally {
      close(ps);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("DDL", log, metric, sql, null, logOptions);
      } else {
        DebugSql.logError("DDL", log, metric, errorCode, sql, null, logOptions);
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
