package edu.stanford.database;

import java.sql.Connection;

import org.jetbrains.annotations.NotNull;

/**
 * Primary class for accessing a relational (SQL) database.
 *
 * @author garricko
 */
public class DatabaseImpl implements Database {
  private final Connection connection;
  private boolean allowTransactions;

  public DatabaseImpl(@NotNull Connection connection) {
    this(connection, false);
  }

  public DatabaseImpl(@NotNull Connection connection, boolean allowTransactions) {
    this.connection = connection;
    this.allowTransactions = allowTransactions;
  }

  @Override
  public @NotNull DatabaseImpl get() {
    return this;
  }

  @Override
  public @NotNull SqlInsert insert(@NotNull String sql) {
    return new SqlInsertImpl(connection, sql);
  }

  @Override
  public @NotNull SqlSelect select(@NotNull String sql) {
    return new SqlSelectImpl(connection, sql);
  }

  @Override
  public @NotNull SqlUpdate update(@NotNull String sql) {
    return new SqlUpdateImpl(connection, sql);
  }

  @Override
  public @NotNull SqlUpdate delete(@NotNull String sql) {
    return new SqlUpdateImpl(connection, sql);
  }

  @Override
  public @NotNull Ddl ddl(@NotNull String sql) {
    return new DdlImpl(connection, sql);
  }

  public void commitNow() {
    if (!allowTransactions) {
      throw new DatabaseException("Calls to commitNow() are not allowed");
    }

    try {
      connection.commit();
    } catch (Exception e) {
      throw new DatabaseException("Unable to commit transaction", e);
    }
  }

  public void rollbackNow() {
    if (!allowTransactions) {
      throw new DatabaseException("Calls to rollbackNow() are not allowed");
    }

    try {
      connection.rollback();
    } catch (Exception e) {
      throw new DatabaseException("Unable to rollback transaction", e);
    }
  }
}
