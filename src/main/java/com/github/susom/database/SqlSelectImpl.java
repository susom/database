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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.MixedParameterSql.RewriteArg;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class SqlSelectImpl implements SqlSelect {
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private final Connection connection;
  private final StatementAdaptor adaptor;
  private PreparedStatement ps; // hold reference to support cancel from another thread
  private final Object cancelLock = new Object();
  private final String sql;
  private final Options options;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args
  private int timeoutSeconds = -1; // -1 ==> no timeout
  private int maxRows = -1; // -1 ==> unlimited

  public SqlSelectImpl(Connection connection, String sql, Options options) {
    this.connection = connection;
    this.sql = sql;
    this.options = options;
    adaptor = new StatementAdaptor(options);
  }

  @NotNull
  @Override
  public SqlSelect argInteger(Integer arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argInteger(@NotNull String argName, Integer arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argLong(Long arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argLong(@NotNull String argName, Long arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argFloat(Float arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argFloat(@NotNull String argName, Float arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argDouble(Double arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argDouble(@NotNull String argName, Double arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argBigDecimal(BigDecimal arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argBigDecimal(@NotNull String argName, BigDecimal arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @NotNull
  @Override
  public SqlSelect argString(String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @NotNull
  @Override
  public SqlSelect argString(@NotNull String argName, String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @NotNull
  @Override
  public SqlSelect argDate(Date arg) {
    return positionalArg(adaptor.nullDate(arg));
  }

  @NotNull
  @Override
  public SqlSelect argDate(@NotNull String argName, Date arg) {
    return namedArg(argName, adaptor.nullDate(arg));
  }

  @NotNull
  @Override
  public SqlSelect argDateNowPerApp() {
    return positionalArg(adaptor.nullDate(options.currentDate()));
  }

  @NotNull
  @Override
  public SqlSelect argDateNowPerApp(@NotNull String argName) {
    return namedArg(argName, adaptor.nullDate(options.currentDate()));
  }

  @NotNull
  @Override
  public SqlSelect argDateNowPerDb() {
    if (options.useDatePerAppOnly()) {
      return positionalArg(adaptor.nullDate(options.currentDate()));
    }
    return positionalArg(new RewriteArg(options.flavor().sysdate()));
  }

  @NotNull
  @Override
  public SqlSelect argDateNowPerDb(@NotNull String argName) {
    if (options.useDatePerAppOnly()) {
      return namedArg(argName, adaptor.nullDate(options.currentDate()));
    }
    return namedArg(argName, new RewriteArg(options.flavor().sysdate()));
  }

  @NotNull
  @Override
  public SqlSelect withTimeoutSeconds(int seconds) {
    timeoutSeconds = seconds;
    return this;
  }

  @NotNull
  @Override
  public SqlSelect withMaxRows(int rows) {
    maxRows = rows;
    return this;
  }

  @NotNull
  @Override
  public SqlSelect apply(Apply apply) {
    apply.apply(this);
    return this;
  }

  @Override
  @Nullable
  public Long queryLongOrNull() {
    return queryWithTimeout(new RowsHandler<Long>() {
      @Override
      public Long process(Rows rs) throws Exception {
        if (rs.next()) {
          return rs.getLongOrNull(1);
        }
        return null;
      }
    });
  }

  @NotNull
  @Override
  public List<Long> queryLongs() {
    return queryWithTimeout(new RowsHandler<List<Long>>() {
      @Override
      public List<Long> process(Rows rs) throws Exception {
        List<Long> result = new ArrayList<>();
        while (rs.next()) {
          Long value = rs.getLongOrNull(1);
          if (value != null) {
            result.add(value);
          }
        }
        return result;
      }
    });
  }

  @Nullable
  @Override
  public Integer queryIntegerOrNull() {
    return queryWithTimeout(new RowsHandler<Integer>() {
      @Override
      public Integer process(Rows rs) throws Exception {
        if (rs.next()) {
          return rs.getIntegerOrNull(1);
        }
        return null;
      }
    });
  }

  @NotNull
  @Override
  public List<Integer> queryIntegers() {
    return queryWithTimeout(new RowsHandler<List<Integer>>() {
      @Override
      public List<Integer> process(Rows rs) throws Exception {
        List<Integer> result = new ArrayList<Integer>();
        while (rs.next()) {
          Integer value = rs.getIntegerOrNull(1);
          if (value != null) {
            result.add(value);
          }
        }
        return result;
      }
    });
  }

  @Override
  public String queryStringOrNull() {
    return queryWithTimeout(new RowsHandler<String>() {
      @Override
      public String process(Rows rs) throws Exception {
        if (rs.next()) {
          return rs.getStringOrNull(1);
        }
        return null;
      }
    });
  }

  @NotNull
  @Override
  public String queryStringOrEmpty() {
    return queryWithTimeout(new RowsHandler<String>() {
      @Override
      public String process(Rows rs) throws Exception {
        if (rs.next()) {
          return rs.getStringOrEmpty(1);
        }
        return "";
      }
    });
  }

  @NotNull
  @Override
  public List<String> queryStrings() {
    return queryWithTimeout(new RowsHandler<List<String>>() {
      @Override
      public List<String> process(Rows rs) throws Exception {
        List<String> result = new ArrayList<>();
        while (rs.next()) {
          String value = rs.getStringOrNull(1);
          if (value != null) {
            result.add(value);
          }
        }
        return result;
      }
    });
  }

  @Nullable
  @Override
  public Date queryDateOrNull() {
    return queryWithTimeout(new RowsHandler<Date>() {
      @Override
      public Date process(Rows rs) throws Exception {
        if (rs.next()) {
          return rs.getDateOrNull(1);
        }
        return null;
      }
    });
  }

  @NotNull
  @Override
  public List<Date> queryDates() {
    return queryWithTimeout(new RowsHandler<List<Date>>() {
      @Override
      public List<Date> process(Rows rs) throws Exception {
        List<Date> result = new ArrayList<>();
        while (rs.next()) {
          Date value = rs.getDateOrNull(1);
          if (value != null) {
            result.add(value);
          }
        }
        return result;
      }
    });
  }

  @Override
  public <T> T query(RowsHandler<T> rowsHandler) {
    return queryWithTimeout(rowsHandler);
  }

  private SqlSelect positionalArg(Object arg) {
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  private SqlSelect namedArg(String argName, Object arg) {
    if (parameterMap == null) {
      parameterMap = new HashMap<>();
    }
    if (argName.startsWith(":")) {
      argName = argName.substring(1);
    }
    parameterMap.put(argName, arg);
    return this;
  }

  private <T> T queryWithTimeout(RowsHandler<T> handler) {
    assert ps == null;
    ResultSet rs = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql = sql;
    Object[] parameters = null;

    boolean isWarn = false;
    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      MixedParameterSql mpSql = new MixedParameterSql(sql, parameterList, parameterMap);
      executeSql = mpSql.getSqlToExecute();
      parameters = mpSql.getArgs();

      synchronized (cancelLock) {
        ps = connection.prepareStatement(executeSql);
      }

      if (timeoutSeconds >= 0) {
        ps.setQueryTimeout(timeoutSeconds);
      }

      if (maxRows > 0) {
        ps.setMaxRows(maxRows);
      }

      adaptor.addParameters(ps, parameters);
      metric.checkpoint("prep");
      rs = ps.executeQuery();
      metric.checkpoint("exec");
      final ResultSet finalRs = rs;
      T result = handler.process(new RowsAdaptor(finalRs));
      metric.checkpoint("read");
      isSuccess = true;
      return result;
    } catch (SQLException e) {
      if (e.getErrorCode() == 1013) {
        isWarn = true;
        // It's ambiguous based on the Oracle error code whether it was a timeout or cancel
        throw new QueryTimedOutException("Timeout of " + timeoutSeconds + " seconds exceeded or user cancelled", e);
      }
      errorCode = options.generateErrorCode();
      logEx = e;
      throw new DatabaseException(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw new DatabaseException(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(rs, log);
      adaptor.closeQuietly(ps, log);
      synchronized (cancelLock) {
        ps = null;
      }
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Query", log, metric, executeSql, parameters, options);
      } else if (isWarn) {
        DebugSql.logWarning("Query", log, metric, "QueryTimedOutException", executeSql, parameters, options, logEx);
      } else {
        DebugSql.logError("Query", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }
}
