package edu.stanford.database;

/**
 * Indicates something went wrong accessing the database. Most often this is
 * used to wrap SQLException to avoid declaring checked exceptions.
 *
 * @author garricko
 */
public class DatabaseException extends RuntimeException {
  public DatabaseException(String message) {
    super(message);
  }

  public DatabaseException(Throwable cause) {
    super(cause);
  }

  public DatabaseException(String message, Throwable cause) {
    super(message, cause);
  }
}
