package com.github.susom.database;

/**
 * Thrown when inserting/updating rows and the actual number of rows modified does
 * not match the expected number of rows.
 *
 * @author garricko
 */
public class WrongNumberOfRowsException extends DatabaseException {
  public WrongNumberOfRowsException(String message) {
    super(message);
  }
}
