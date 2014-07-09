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

import org.jetbrains.annotations.NotNull;

/**
 * Primary class for accessing a relational (SQL) database.
 *
 * @author garricko
 */
public class DatabaseImpl implements Database {
  private final Connection connection;
  private final Options options;

  public DatabaseImpl(@NotNull Connection connection, @NotNull Options options) {
    this.connection = connection;
    this.options = options;
  }

  @Override
  @NotNull
  public DatabaseImpl get() {
    return this;
  }

  @Override
  @NotNull
  public SqlInsert insert(@NotNull String sql) {
    return new SqlInsertImpl(connection, sql, options);
  }

  @Override
  @NotNull
  public SqlSelect select(@NotNull String sql) {
    return new SqlSelectImpl(connection, sql, options);
  }

  @Override
  @NotNull
  public SqlUpdate update(@NotNull String sql) {
    return new SqlUpdateImpl(connection, sql, options);
  }

  @Override
  @NotNull
  public SqlUpdate delete(@NotNull String sql) {
    return new SqlUpdateImpl(connection, sql, options);
  }

  @Override
  @NotNull
  public Ddl ddl(@NotNull String sql) {
    return new DdlImpl(connection, sql, options);
  }

  public void commitNow() {
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
    if (!options.allowTransactionControl()) {
      throw new DatabaseException("Calls to rollbackNow() are not allowed");
    }

    try {
      connection.rollback();
    } catch (Exception e) {
      throw new DatabaseException("Unable to rollback transaction", e);
    }
  }

  @NotNull
  @Override
  public Flavor flavor() {
    return options.flavor();
  }

  @Override
  public void dropSequenceQuietly(@NotNull String sequenceName) {
    ddl(flavor().sequenceDrop(sequenceName)).executeQuietly();
  }

  @Override
  public void dropTableQuietly(@NotNull String tableName) {
    ddl("drop table " + tableName).executeQuietly();
  }
}
