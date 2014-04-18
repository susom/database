package com.github.susom.database;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Convenience class to substitute real values into a database query for debugging, logging, etc.
 *
 * WARNING!!! Never execute this SQL without manual inspection because this class does NOTHING
 * to prevent SQL injection or any other bad things.
 *
 * @author garricko
 */
public class DebugSql {
  private final String sql;
  private final Object[] args;

  public DebugSql(String sql, Object[] args) {
    this.sql = sql;
    this.args = args;
  }

  @Override
  public String toString() {
    Object[] argsToPrint = this.args;
    if (argsToPrint == null) {
      argsToPrint = new Object[0];
    }
    int batchSize = -1;
    if (argsToPrint.length > 0 && argsToPrint instanceof Object[][]) {
      // The arguments provided were from a batch - just use the first set
      batchSize = argsToPrint.length;
      argsToPrint = (Object[]) argsToPrint[0];
    }
    String[] sqlParts = sql.split("\\?");
    StringBuilder buf = new StringBuilder();
    if (sqlParts.length != argsToPrint.length + (sql.endsWith("?") ? 0 : 1)) {
      buf.append("(wrong # args) query: ");
      buf.append(sql);
      buf.append(" args: ");
      buf.append(Arrays.toString(argsToPrint));
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      buf.append(sql);
      buf.append('|');
      for (int i = 0; i < argsToPrint.length; i++) {
        buf.append(sqlParts[i]);
        if (argsToPrint[i] instanceof String) {
          buf.append("'");
          buf.append(argsToPrint[i]);
          buf.append("'");
        } else if (argsToPrint[i] instanceof Date) {
          buf.append("to_date('");
          buf.append(sdf.format((Date) argsToPrint[i]));
          buf.append("', 'YYYY-MM-DD')");
        } else {
          buf.append(argsToPrint[i]);
        }
      }
      if (sqlParts.length > argsToPrint.length) {
        buf.append(sqlParts[sqlParts.length - 1]);
      }
    }
    if (batchSize != -1) {
      buf.append(" (first in batch of ");
      buf.append(batchSize);
      buf.append(')');
    }
    return buf.toString();
  }
}
