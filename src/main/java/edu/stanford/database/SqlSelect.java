package edu.stanford.database;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlSelect {
  SqlSelect argInteger(Integer arg);

  SqlSelect argInteger(String argName, Integer arg);

  SqlSelect argLong(Long arg);

  SqlSelect argLong(String argName, Long arg);

  SqlSelect argFloat(Float arg);

  SqlSelect argFloat(String argName, Float arg);

  SqlSelect argDouble(Double arg);

  SqlSelect argDouble(String argName, Double arg);

  SqlSelect argBigDecimal(BigDecimal arg);

  SqlSelect argBigDecimal(String argName, BigDecimal arg);

  SqlSelect argString(String arg);

  SqlSelect argString(String argName, String arg);

  SqlSelect argDate(Date arg);

  SqlSelect argDate(String argName, Date arg);

  SqlSelect withTimeoutSeconds(int seconds);

  SqlSelect withMaxRows(int rows);

  Long queryLong();

  List<Long> queryLongs();

  <T> T query(RowsHandler<T> rowsHandler);
}
