package com.github.susom.database;

/**
 * Something that can provide a string value based on a string key.
 *
 * @author garricko
 */
public interface ConfigStrings {
  /**
   * Provide access to some configuration parameter.
   *
   * @param key name for this lookup
   * @return value corresponding to the key, or null; should never return an empty string
   */
  String get(String key);
}
