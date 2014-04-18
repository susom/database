package com.github.susom.database;

/**
 * Thrown when a query is interrupted because a timeout was exceeded or it was
 * explicitly cancelled.
 *
 * @author garricko
 */
public class QueryTimedOutException extends DatabaseException {
  public QueryTimedOutException(String message, Throwable cause) {
    super(message, cause);
  }
}
