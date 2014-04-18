package com.github.susom.database;

import javax.inject.Provider;

/**
 * Primary class for accessing a relational (SQL) database.
 *
 * @author garricko
 */
public interface Database extends Provider<Database> {
  SqlInsert insert(String sql);

  SqlSelect select(String sql);

  SqlUpdate update(String sql);

  SqlUpdate delete(String sql);

  Ddl ddl(String sql);

  void commitNow();

  void rollbackNow();
}
