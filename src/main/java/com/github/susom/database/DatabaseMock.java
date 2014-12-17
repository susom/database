package com.github.susom.database;

/**
 * Convenience class to intercept calls to Connection and return stubbed results
 * for testing purposes.
 */
public interface DatabaseMock {
  RowStub query(String executeSql, String debugSql);

  Integer insert(String executeSql, String debugSql);

  Long insertReturningPk(String executeSql, String debugSql);

  RowStub insertReturning(String executeSql, String debugSql);

  Integer update(String executeSql, String debugSql);
}
