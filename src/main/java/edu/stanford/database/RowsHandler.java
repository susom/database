package edu.stanford.database;

/**
 * Type-safe callback to read query results.
 *
 * @author garricko
 */
public interface RowsHandler<T> {
  T process(Rows rs) throws Exception;
}
