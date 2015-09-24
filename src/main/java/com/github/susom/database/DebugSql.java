/*
 * Copyright 2014 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.susom.database;

import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;

import com.github.susom.database.MixedParameterSql.SecretArg;

/**
 * Convenience class to substitute real values into a database query for debugging, logging, etc.
 * <p/>
 * WARNING!!! Never execute this SQL without manual inspection because this class does NOTHING
 * to prevent SQL injection or any other bad things.
 *
 * @author garricko
 */
public class DebugSql {
  public static final String PARAM_SQL_SEPARATOR = "\tParamSql:\t";

  public static String printDebugOnlySqlString(String sql, Object[] args, Options options) {
    StringBuilder buf = new StringBuilder();
    printSql(buf, sql, args, false, true, options.flavor());
    return buf.toString();
  }

  public static void printSql(StringBuilder buf, String sql, Object[] args, Options options) {
    printSql(buf, sql, args, true, options.isLogParameters(), options.flavor());
  }

  public static void printSql(StringBuilder buf, String sql, Object[] args, boolean includeExecSql,
                              boolean includeParameters, Flavor flavor) {
    Object[] argsToPrint = args;
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
    if (sqlParts.length != argsToPrint.length + (sql.endsWith("?") ? 0 : 1)) {
      buf.append("(wrong # args) query: ");
      buf.append(sql);
      if (args != null) {
        buf.append(" args: ");
        if (includeParameters) {
          buf.append(Arrays.toString(argsToPrint));
        } else {
          buf.append(argsToPrint.length);
        }
      }
    } else {
      if (includeExecSql) {
        buf.append(removeTabs(sql));
      }
      if (includeParameters && argsToPrint.length > 0) {
        if (includeExecSql) {
          buf.append(PARAM_SQL_SEPARATOR);
        }
        for (int i = 0; i < argsToPrint.length; i++) {
          buf.append(removeTabs(sqlParts[i]));
          Object argToPrint = argsToPrint[i];
          if (argToPrint instanceof String) {
            buf.append("'");
            buf.append(removeTabs(escapeSingleQuoted(((String) argToPrint))));
            buf.append("'");
          } else if (argToPrint instanceof StatementAdaptor.SqlNull || argToPrint == null) {
            buf.append("null");
          } else if (argToPrint instanceof Date) {
            buf.append(flavor.dateAsSqlFunction((Date) argToPrint));
          } else if (argToPrint instanceof Number) {
            buf.append(argToPrint);
          } else if (argToPrint instanceof Boolean) {
            buf.append(((Boolean) argToPrint) ? "'Y'" : "'N'");
          } else if (argToPrint instanceof SecretArg) {
            buf.append("<secret>");
          } else {
            buf.append("<unknown:").append(argToPrint.getClass().getName()).append(">");
          }
        }
        if (sqlParts.length > argsToPrint.length) {
          buf.append(sqlParts[sqlParts.length - 1]);
        }
      }
    }
    if (batchSize != -1) {
      buf.append(" (first in batch of ");
      buf.append(batchSize);
      buf.append(')');
    }
  }

  private static String removeTabs(String s) {
    return s == null ? null : s.replace("\t", "<tab>");
  }

  private static String escapeSingleQuoted(String s) {
    return s == null ? null : s.replace("'", "''");
  }

  public static String exceptionMessage(String sql, Object[] parameters, String errorCode, Options options) {
    StringBuilder buf = new StringBuilder("Error executing SQL");
    if (errorCode != null) {
      buf.append(" (errorCode=").append(errorCode).append(")");
    }
    if (options.isDetailedExceptions()) {
      buf.append(": ");
      DebugSql.printSql(buf, sql, parameters, options);
    }
    return buf.toString();
  }

  public static void logSuccess(String sqlType, Logger log, Metric metric, String sql, Object[] args, Options options) {
    if (log.isDebugEnabled()) {
      StringBuilder buf = new StringBuilder();
      buf.append(sqlType).append(": ");
      metric.printMessage(buf);
      buf.append('\t');
      printSql(buf, sql, args, options);
      log.debug(buf.toString());
    }
  }

  public static void logWarning(String sqlType, Logger log, Metric metric, String errorCode, String sql, Object[] args,
                          Options options, Throwable t) {
    if (log.isWarnEnabled()) {
      StringBuilder buf = new StringBuilder();
      if (errorCode != null) {
        buf.append("errorCode=").append(errorCode).append(" ");
      }
      buf.append(sqlType).append(": ");
      metric.printMessage(buf);
      buf.append(" ");
      printSql(buf, sql, args, options);
      log.warn(buf.toString(), t);
    }
  }

  public static void logError(String sqlType, Logger log, Metric metric, String errorCode, String sql, Object[] args,
                        Options options, Throwable t) {
    if (log.isErrorEnabled()) {
      StringBuilder buf = new StringBuilder();
      if (errorCode != null) {
        buf.append("errorCode=").append(errorCode).append(" ");
      }
      buf.append(sqlType).append(": ");
      metric.printMessage(buf);
      buf.append(" ");
      printSql(buf, sql, args, options);
      log.error(buf.toString(), t);
    }
  }
}
