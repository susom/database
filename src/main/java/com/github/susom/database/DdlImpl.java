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
        log.debug("DDL: " + metric.getMessage() + " " + sql);
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
    return "Error executing SQL: " + sql;
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
