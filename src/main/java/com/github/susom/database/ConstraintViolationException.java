package com.github.susom.database;

/**
 * This exception will be thrown when a condition arises that violates
 * a stated invariant regarding the database. This might be a database
 * schema "constraint violated" as thrown by the database, or could be
 * caused by a violation of constraints enforced only within the code.
 */
public class ConstraintViolationException extends DatabaseException {
  public ConstraintViolationException(String message) {
    super(message);
  }

  public ConstraintViolationException(Throwable cause) {
    super(cause);
  }

  public ConstraintViolationException(String message, Throwable cause) {
    super(message, cause);
  }
}
