package edu.stanford.database;

/**
 * Interface for executing a chunk of DDL within the database.
 *
 * @author garricko
 */
public interface Ddl {
  /**
   * Execute the DDL statement. All checked SQLExceptions get wrapped in DatabaseExceptions.
   */
  void execute();

  /**
   * This just does an execute() call and silently discards any DatabaseException
   * that might occur. This can be useful for things like drop statements, where
   * some databases don't make it easy to conditionally drop things only if they
   * exist.
   */
  void executeQuietly();
}
