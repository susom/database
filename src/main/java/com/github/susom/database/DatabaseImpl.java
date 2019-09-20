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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*>>>
import org.checkerframework.checker.tainting.qual.Untainted;
*/

/**
 * Primary class for accessing a relational (SQL) database.
 *
 * @author garricko
 */
public class DatabaseImpl implements Database {
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private final Connection connection;
  private final DatabaseMock mock;
  private final Options options;

  public DatabaseImpl(@Nonnull Connection connection, @Nonnull Options options) {
    this.connection = connection;
    this.mock = null;
    this.options = options;
  }

  public DatabaseImpl(@Nonnull DatabaseMock mock, Flavor flavor) {
    this(mock, new OptionsDefault(flavor));
  }

  public DatabaseImpl(@Nonnull DatabaseMock mock, @Nonnull Options options) {
    this.connection = null;
    this.mock = mock;
    this.options = options;
  }

  @Override
  @Nonnull
  public DatabaseImpl get() {
    return this;
  }

  @Override
  @Nonnull
  public SqlInsert toInsert(@Nonnull String sql) {
    return new SqlInsertImpl(connection, mock, sql, options);
  }

  @Nonnull
  @Override
  public SqlInsert toInsert(@Nonnull Sql sql) {
    return new SqlInsertImpl(connection, mock, sql.sql(), options).apply(sql);
  }

  @Override
  @Nonnull
  public SqlSelect toSelect(@Nonnull String sql) {
    return new SqlSelectImpl(connection, mock, sql, options);
  }

  @Nonnull
  @Override
  public SqlSelect toSelect(@Nonnull Sql sql) {
    return new SqlSelectImpl(connection, mock, sql.sql(), options).apply(sql);
  }

  @Override
  @Nonnull
  public SqlUpdate toUpdate(@Nonnull String sql) {
    return new SqlUpdateImpl(connection, mock, sql, options);
  }

  @Nonnull
  @Override
  public SqlUpdate toUpdate(@Nonnull Sql sql) {
    return new SqlUpdateImpl(connection, mock, sql.sql(), options).apply(sql);
  }

  @Override
  @Nonnull
  public SqlUpdate toDelete(@Nonnull String sql) {
    return new SqlUpdateImpl(connection, mock, sql, options);
  }

  @Nonnull
  @Override
  public SqlUpdate toDelete(@Nonnull Sql sql) {
    return new SqlUpdateImpl(connection, mock, sql.sql(), options).apply(sql);
  }

  @Override
  @Nonnull
  public Ddl ddl(@Nonnull String sql) {
    return new DdlImpl(connection, sql, options);
  }

  @Override
  public Long nextSequenceValue(/*@Untainted*/ @Nonnull String sequenceName) {
    return toSelect(flavor().sequenceSelectNextVal(sequenceName)).queryLongOrNull();
  }

  @Override
  public Date nowPerApp() {
    return options.currentDate();
  }

  public void commitNow() {
    if (options.ignoreTransactionControl()) {
      log.debug("Ignoring call to commitNow()");
      return;
    }

    if (!options.allowTransactionControl()) {
      throw new DatabaseException("Calls to commitNow() are not allowed");
    }

    try {
      connection.commit();
    } catch (Exception e) {
      throw new DatabaseException("Unable to commit transaction", e);
    }
  }

  public void rollbackNow() {
    if (options.ignoreTransactionControl()) {
      log.debug("Ignoring call to rollbackNow()");
      return;
    }

    if (!options.allowTransactionControl()) {
      throw new DatabaseException("Calls to rollbackNow() are not allowed");
    }

    try {
      connection.rollback();
    } catch (Exception e) {
      throw new DatabaseException("Unable to rollback transaction", e);
    }
  }

  @Nonnull
  @Override
  public Connection underlyingConnection() {
    if (!options.allowConnectionAccess()) {
      throw new DatabaseException("Calls to underlyingConnection() are not allowed");
    }

    return connection;
  }

  @Nonnull
  @Override
  public Options options() {
    return options;
  }

  @Nonnull
  @Override
  public Flavor flavor() {
    return options.flavor();
  }

  @Nonnull
  @Override
  public When when() {
    return new When(options.flavor());
  }

  @Override
  public void dropSequenceQuietly(/*@Untainted*/ @Nonnull String sequenceName) {
    ddl(flavor().sequenceDrop(sequenceName)).executeQuietly();
  }

  @Override
  public void dropTableQuietly(/*@Untainted*/ @Nonnull String tableName) {
    if (flavor() == Flavor.postgresql || flavor() == Flavor.hsqldb) {
      ddl("drop table if exists " + tableName).executeQuietly();
    } else {
      ddl("drop table " + tableName).executeQuietly();
    }
  }

  @Override
  public boolean tableExists(@Nonnull String tableName) throws DatabaseException {

    String schemaName = null;
    Method getSchema = null;

    try {
      // Use reflections to see if connection.getSchema API exists. It should exist for any JDBC7 or later implementation
      // We still support Oracle 11 with odbc6, however, so we can't assume it's there.
      getSchema = connection.getClass().getDeclaredMethod("getSchema", new Class[0]);
    } catch (NoSuchMethodException noMethodExc) {
      // Expected if method does not exist - just let it go
    }

    try {
      if (getSchema != null) {
        schemaName = ((String) getSchema.invoke(connection, new Object[0]));

      } else if (flavor() == Flavor.oracle) {
        // Oracle defaults to user name schema - use that.
        log.warn("Connection getSchema API was not found.  Defaulting to Oracle user name schema." +
          "If this is not appropriate, please use tableExists(tableName, schemaName) API or upgrade to ojdbc7 or later");
        schemaName = connection.getMetaData().getUserName();
      }

      if (schemaName == null) {
        // connection.getSchema API was supported starting at JDK1.7.  Method should not be null.
        throw new NullPointerException("Unable to retrieve schema name.");
      }

    } catch (Exception exc) {
      throw new DatabaseException("Unable to determine the schema. " +
        "Please use tableExists(tableName, schemaName API) or upgrade to a JDBC7 driver or later.", exc);
    }

    return tableExists(tableName, schemaName);
  }


  @Override
  public boolean tableExists(@Nonnull String tableName, String schemaName) throws DatabaseException {
    if (tableName != null) {
      try {
        DatabaseMetaData metadata = connection.getMetaData();
        String normalizedTable = normalizeTableName(tableName);
        ResultSet resultSet =
          metadata.getTables(connection.getCatalog(), schemaName, normalizedTable, new String[]{"TABLE", "VIEW"});

        while (resultSet.next()) {
          if (normalizedTable.equals(resultSet.getString("TABLE_NAME"))) {
            return true;
          }
        }
      } catch (SQLException exc) {
        throw new DatabaseException("Unable to look up table " + tableName
          + " in schema  " + schemaName + " : " + exc.getMessage(),
          exc);
      }
    }

    return false;
  }

  @Override
  public String normalizeTableName(String tableName) {
    if (tableName == null) {
      return tableName;
    }

    // If user gave us a quoted string, leave it alone for look up
    if (tableName.length() > 2) {
      if (tableName.startsWith("\"") && tableName.endsWith("\"")) {
        // Remove quotes and return as is.
        return tableName.substring(1, tableName.length()-1);
      }
    }

    if (flavor().isNormalizedUpperCase()) {
      return tableName.toUpperCase();
    }

    return tableName.toLowerCase();
  }

  @Override
  public void assertTimeSynchronized(long millisToWarn, long millisToError) {
    toSelect("select ?" + flavor().fromAny())
        .argDateNowPerDb().queryFirstOrNull(r -> {
      Date appDate = nowPerApp();
      Date dbDate = r.getDateOrNull();

      if (dbDate == null) {
        throw new DatabaseException("Expecting a date in the result");
      }

      if (Math.abs(appDate.getTime() - dbDate.getTime()) > 3600000) {
        throw new DatabaseException("App and db time are over an hour apart (check your timezones) app: "
            + DateTimeFormatter.ISO_INSTANT.format(appDate.toInstant()) + " db: "
            + DateTimeFormatter.ISO_INSTANT.format(dbDate.toInstant()));
      }

      if (Math.abs(appDate.getTime() - dbDate.getTime()) > millisToError) {
        throw new DatabaseException("App and db time over " + millisToError + " millis apart (check your clocks) app: "
            + DateTimeFormatter.ISO_INSTANT.format(appDate.toInstant()) + " db: "
            + DateTimeFormatter.ISO_INSTANT.format(dbDate.toInstant()));
      }

      if (Math.abs(appDate.getTime() - dbDate.getTime()) > millisToWarn) {
        log.warn("App and db time are over " + millisToWarn + " millis apart (check your clocks) app: "
            + DateTimeFormatter.ISO_INSTANT.format(appDate.toInstant()) + " db: "
            + DateTimeFormatter.ISO_INSTANT.format(dbDate.toInstant()));
      }

      return null;
    });
  }

  @Override
  public void assertTimeSynchronized() {
    assertTimeSynchronized(10000, 30000);
  }
}
