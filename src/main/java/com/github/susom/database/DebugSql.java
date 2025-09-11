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

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
    printSql(buf, sql, args, false, true, options);
    return buf.toString();
  }

  public static void printSql(StringBuilder buf, String sql, Object[] args, Options options) {
    printSql(buf, sql, args, true, options.isLogParameters(), options);
  }

  public static void printSql(StringBuilder buf, String sql, Object[] args, boolean includeExecSql,
                              boolean includeParameters, Options options) {
    printSql(buf, sql, args, includeExecSql, includeParameters, true, options);
  }

  public static void printSql(StringBuilder buf, String sql, Object[] args, boolean includeExecSql,
                              boolean includeParameters, boolean validateParameters, Options options) {
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

    if (!validateParameters && argsToPrint.length > 0) {
      // When validation is disabled (e.g., for logging post-processed SQL from MixedParameterSql),
      // we need to carefully substitute parameters. The SQL may contain literal ? characters
      // from escaped parameters, so we only substitute the last N ? characters where N is the
      // number of arguments.
      if (includeExecSql) {
        buf.append(removeTabs(sql));
      }
      if (includeParameters) {
        if (includeExecSql) {
          buf.append(PARAM_SQL_SEPARATOR);
        }
        
        // Find all question mark positions
        List<Integer> questionMarkPositions = new ArrayList<>();
        for (int i = 0; i < sql.length(); i++) {
          if (sql.charAt(i) == '?') {
            questionMarkPositions.add(i);
          }
        }
        
        // Only substitute the last N question marks, where N is the number of arguments
        int numParamsToSubstitute = Math.min(argsToPrint.length, questionMarkPositions.size());
        List<Integer> paramPositions = new ArrayList<>();
        if (numParamsToSubstitute > 0) {
          for (int i = questionMarkPositions.size() - numParamsToSubstitute; i < questionMarkPositions.size(); i++) {
            paramPositions.add(questionMarkPositions.get(i));
          }
        }
        
        // Build the result by substituting at the identified positions
        int lastPos = 0;
        int argIndex = 0;
        for (int pos : paramPositions) {
          // Add SQL up to this parameter position
          buf.append(removeTabs(sql.substring(lastPos, pos)));
          
          // Add the parameter value
          if (argIndex < argsToPrint.length) {
            appendParameterValue(buf, argsToPrint[argIndex], options);
            argIndex++;
          }
          
          lastPos = pos + 1; // Skip the ? character
        }
        
        // Add remaining SQL after last parameter
        if (lastPos < sql.length()) {
          buf.append(removeTabs(sql.substring(lastPos)));
        }
      }
    } else {
      String[] sqlParts = sql.split("\\?");
      if (validateParameters && sqlParts.length != argsToPrint.length + (sql.endsWith("?") ? 0 : 1)) {
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
            appendParameterValue(buf, argsToPrint[i], options);
          }
          if (sqlParts.length > argsToPrint.length) {
            buf.append(sqlParts[sqlParts.length - 1]);
          }
        }
      }
    }
    if (batchSize != -1) {
      buf.append(" (first in batch of ");
      buf.append(batchSize);
      buf.append(')');
    }
  }

  private static void appendParameterValue(StringBuilder buf, Object argToPrint, Options options) {
    if (argToPrint instanceof String) {
      String argToPrintString = (String) argToPrint;
      int maxLength = options.maxStringLengthParam();
      if (argToPrintString.length() > maxLength && maxLength > 0) {
        buf.append("'").append(argToPrintString.substring(0, maxLength)).append("...'");
      } else {
        buf.append("'");
        buf.append(removeTabs(escapeSingleQuoted(argToPrintString)));
        buf.append("'");
      }
    } else if (argToPrint instanceof StatementAdaptor.SqlNull || argToPrint == null) {
      buf.append("null");
    } else if (argToPrint instanceof java.sql.Timestamp) {
      buf.append(options.flavor().dateAsSqlFunction((Date) argToPrint, options.calendarForTimestamps()));
    } else if (argToPrint instanceof java.sql.Date) {
        buf.append(options.flavor().localDateAsSqlFunction((Date) argToPrint));
    } else if (argToPrint instanceof Number) {
      buf.append(argToPrint);
    } else if (argToPrint instanceof Boolean) {
      buf.append(((Boolean) argToPrint) ? "'Y'" : "'N'");
    } else if (argToPrint instanceof SecretArg) {
      buf.append("<secret>");
    } else if (argToPrint instanceof InternalStringReader) {
      String argToPrintString = ((InternalStringReader) argToPrint).getString();
      int maxLength = options.maxStringLengthParam();
      if (argToPrintString.length() > maxLength && maxLength > 0) {
        buf.append("'").append(argToPrintString.substring(0, maxLength)).append("...'");
      } else {
        buf.append("'");
        buf.append(removeTabs(escapeSingleQuoted(argToPrintString)));
        buf.append("'");
      }
    } else if (argToPrint instanceof Reader || argToPrint instanceof InputStream) {
      buf.append("<").append(argToPrint.getClass().getName()).append(">");
    } else if (argToPrint instanceof byte[]) {
      buf.append("<").append(((byte[]) argToPrint).length).append(" bytes>");
    } else {
      buf.append("<unknown:").append(argToPrint.getClass().getName()).append(">");
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
      String msg = logMiddle('\t', sqlType, metric, null, sql, args, options);
      log.debug(msg);
    }
  }

  public static void logWarning(String sqlType, Logger log, Metric metric, String errorCode, String sql, Object[] args,
                          Options options, Throwable t) {
    if (log.isWarnEnabled()) {
      String msg = logMiddle(' ', sqlType, metric, errorCode, sql, args, options);
      log.warn(msg, t);
    }
  }

  public static void logError(String sqlType, Logger log, Metric metric, String errorCode, String sql, Object[] args,
                        Options options, Throwable t) {
    if (log.isErrorEnabled()) {
      String msg = logMiddle(' ', sqlType, metric, errorCode, sql, args, options);
      log.error(msg, t);
    }
  }

  private static String logMiddle(char separator, String sqlType, Metric metric,
                                 String errorCode, String sql, Object[] args, Options options) {
    StringBuilder buf = new StringBuilder();
    if (errorCode != null) {
      buf.append("errorCode=").append(errorCode).append(" ");
    }
    buf.append(sqlType).append(": ");
    metric.printMessage(buf);
    buf.append(separator);
    printSql(buf, sql, args, true, options.isLogParameters(), false, options);
    return buf.toString();
  }
}
