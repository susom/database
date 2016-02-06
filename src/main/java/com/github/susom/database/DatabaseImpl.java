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
    if (flavor() == Flavor.postgresql) {
      ddl("drop table if exists " + tableName).executeQuietly();
    } else {
      ddl("drop table " + tableName).executeQuietly();
    }
  }
}
