package com.github.susom.database;

/**
 * Indicates that a configuration value is required but was not present.
 *
 * @author garricko
 */
public class ConfigMissingException extends DatabaseException {
  public ConfigMissingException(String message) {
    super(message);
  }

  public ConfigMissingException(Throwable cause) {
    super(cause);
  }

  public ConfigMissingException(String message, Throwable cause) {
    super(message, cause);
  }
}
