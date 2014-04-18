package com.github.susom.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Date;

/**
 * Interface for configuring (setting parameters) and executing a chunk of SQL.
 *
 * @author garricko
 */
public interface SqlInsert {
  SqlInsert argInteger(Integer arg);

  SqlInsert argInteger(String argName, Integer arg);

  SqlInsert argLong(Long arg);

  SqlInsert argLong(String argName, Long arg);

  SqlInsert argFloat(Float arg);

  SqlInsert argFloat(String argName, Float arg);

  SqlInsert argDouble(Double arg);

  SqlInsert argDouble(String argName, Double arg);

  SqlInsert argBigDecimal(BigDecimal arg);

  SqlInsert argBigDecimal(String argName, BigDecimal arg);

  SqlInsert argString(String arg);

  SqlInsert argString(String argName, String arg);

  SqlInsert argDate(Date arg);

  SqlInsert argDate(String argName, Date arg);

  SqlInsert argBlobBytes(byte[] arg);

  SqlInsert argBlobBytes(String argName, byte[] arg);

  SqlInsert argBlobStream(InputStream arg);

  SqlInsert argBlobStream(String argName, InputStream arg);

  SqlInsert argClobString(String arg);

  SqlInsert argClobString(String argName, String arg);

  SqlInsert argClobReader(Reader arg);

  SqlInsert argClobReader(String argName, Reader arg);

  /**
   * Call this between setting rows of parameters for a SQL statement. You may call it before
   * setting any parameters, after setting all, or multiple times between rows.
   */
//  SqlInsert batch();

  int insert();

  void insert(int expectedRowsUpdated);
}
