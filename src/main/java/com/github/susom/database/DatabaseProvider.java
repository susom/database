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

import java.sql.Connection;
import java.sql.SQLException;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a lazy provider for Database instances. It helps avoid allocating connection
 * or transaction resources until (or if) we actually need a Database. As a consequence
 * of this laziness, the underlying resources require explicit cleanup by calling either
 * commitAndClose() or rollbackAndClose().
 *
 * @author garricko
 */
public final class DatabaseProvider implements Provider<Database> {
  private static final Logger log = LoggerFactory.getLogger(DatabaseProvider.class);
  private Provider<Connection> connectionProvider;
  private boolean txStarted = false;
  private Connection connection = null;
  private Database database = null;
  private boolean allowTxManage;

  public DatabaseProvider(Provider<Connection> connectionProvider) {
    this(connectionProvider, false);
  }

  public DatabaseProvider(Provider<Connection> connectionProvider, boolean allowTxManage) {
    this.connectionProvider = connectionProvider;
    this.allowTxManage = allowTxManage;
  }

  public Database get() {
    if (database != null) {
      return database;
    }

    Metric metric = new Metric(log.isDebugEnabled());
    try {
      connection = connectionProvider.get();
      txStarted = true;
      metric.checkpoint("getConn");
      try {
        if (!connection.getAutoCommit()) {
          connection.setAutoCommit(false);
          metric.checkpoint("setAutoCommit");
        } else {
          metric.checkpoint("checkAutoCommit");
        }
      } catch (SQLException e) {
        throw new DatabaseException("Unable to check/set autoCommit for the connection", e);
      }
      database = new DatabaseImpl(connection, allowTxManage);
      metric.checkpoint("dbInit");
    } catch (RuntimeException e) {
      metric.checkpoint("fail");
      throw e;
    } finally {
      metric.done();
      if (log.isDebugEnabled()) {
        log.debug("Get database: " + metric.getMessage());
      }
    }
    return database;
  }

  public void commitAndClose() {
    if (txStarted) {
      try {
        connection.commit();
      } catch (Exception e) {
        log.error("Unable to commit the transaction", e);
      }
      close();
    }
  }

  public void rollbackAndClose() {
    if (txStarted) {
      try {
        connection.rollback();
      } catch (Exception e) {
        log.error("Unable to rollback the transaction", e);
      }
      close();
    }
  }

  private void close() {
    try {
      connection.close();
    } catch (Exception e) {
      log.error("Unable to close the database connection", e);
    }
  }
}