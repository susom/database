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
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.susom.database.MixedParameterSql.RewriteArg;

/**
 * This is the key class for configuring (query parameters) and executing a database query.
 *
 * @author garricko
 */
public class SqlInsertImpl implements SqlInsert {
  private static final Logger log = LoggerFactory.getLogger(Database.class);
  private final Connection connection;
  private final DatabaseMock mock;
  private final StatementAdaptor adaptor;
  private final String sql;
  private final Options options;
  private List<Batch> batched;
  private List<Object> parameterList;       // !null ==> traditional ? args
  private Map<String, Object> parameterMap; // !null ==> named :abc args
  private String pkArgName;
  private int pkPos;
  private String pkSeqName;
  private Long pkLong;

  public SqlInsertImpl(Connection connection, DatabaseMock mock, String sql, Options options) {
    this.connection = connection;
    this.mock = mock;
    this.sql = sql;
    this.options = options;
    adaptor = new StatementAdaptor(options);
  }

  @Nonnull
  @Override
  public SqlInsert argBoolean(Boolean arg) {
    return positionalArg(adaptor.nullString(booleanToString(arg)));
  }

  @Nonnull
  @Override
  public SqlInsert argBoolean(@Nonnull String argName, Boolean arg) {
    return namedArg(argName, adaptor.nullString(booleanToString(arg)));
  }

  @Override
  @Nonnull
  public SqlInsert argInteger(Integer arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argInteger(@Nonnull String argName, Integer arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argLong(Long arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argLong(@Nonnull String argName, Long arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argFloat(Float arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argFloat(@Nonnull String argName, Float arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argDouble(Double arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argDouble(@Nonnull String argName, Double arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argBigDecimal(BigDecimal arg) {
    return positionalArg(adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argBigDecimal(@Nonnull String argName, BigDecimal arg) {
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argString(String arg) {
    return positionalArg(adaptor.nullString(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argString(@Nonnull String argName, String arg) {
    return namedArg(argName, adaptor.nullString(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argDate(Date arg) {
    return positionalArg(adaptor.nullDate(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argDate(@Nonnull String argName, Date arg) {
    return namedArg(argName, adaptor.nullDate(arg));
  }

  @Nonnull
  @Override
  public SqlInsert argDateNowPerApp() {
    return positionalArg(adaptor.nullDate(options.currentDate()));
  }

  @Override
  @Nonnull
  public SqlInsert argDateNowPerApp(@Nonnull String argName) {
    return namedArg(argName, adaptor.nullDate(options.currentDate()));
  }

  @Nonnull
  @Override
  public SqlInsert argDateNowPerDb() {
    if (options.useDatePerAppOnly()) {
      return positionalArg(adaptor.nullDate(options.currentDate()));
    }
    return positionalArg(new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @Nonnull
  public SqlInsert argDateNowPerDb(@Nonnull String argName) {
    if (options.useDatePerAppOnly()) {
      return namedArg(argName, adaptor.nullDate(options.currentDate()));
    }
    return namedArg(argName, new RewriteArg(options.flavor().dbTimeMillis()));
  }

  @Override
  @Nonnull
  public SqlInsert argBlobBytes(byte[] arg) {
    return positionalArg(adaptor.nullBytes(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argBlobBytes(@Nonnull String argName, byte[] arg) {
    return namedArg(argName, adaptor.nullBytes(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argBlobStream(InputStream arg) {
    return positionalArg(adaptor.nullInputStream(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argBlobStream(@Nonnull String argName, InputStream arg) {
    return namedArg(argName, adaptor.nullInputStream(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argClobString(String arg) {
    return positionalArg(adaptor.nullClobReader(arg == null ? null : new StringReader(arg)));
  }

  @Override
  @Nonnull
  public SqlInsert argClobString(@Nonnull String argName, String arg) {
    return namedArg(argName, adaptor.nullClobReader(arg == null ? null : new StringReader(arg)));
  }

  @Override
  @Nonnull
  public SqlInsert argClobReader(Reader arg) {
    return positionalArg(adaptor.nullClobReader(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argClobReader(@Nonnull String argName, Reader arg) {
    return namedArg(argName, adaptor.nullClobReader(arg));
  }

  @Nonnull
  @Override
  public SqlInsert withArgs(SqlArgs args) {
    return apply(args);
  }

  @Nonnull
  @Override
  public SqlInsert apply(Apply apply) {
    apply.apply(this);
    return this;
  }

  @Override
  public SqlInsert batch() {
    if (!parameterList.isEmpty() || !parameterMap.isEmpty()) {
      if (batched == null) {
        batched = new ArrayList<>();
      }
      batched.add(new Batch(parameterList, parameterMap));
      parameterList = new ArrayList<>();
      parameterMap = new HashMap<>();
    }
    return this;
  }

  @Override
  public int insert() {
    return updateInternal(0);
  }

  @Override
  public void insert(int expectedRowsUpdated) {
    updateInternal(expectedRowsUpdated);
  }

  @Override
  public void insertBatch() {
    int[] result = updateBatch();
    for (int r : result) {
      // Tolerate SUCCESS_NO_INFO for older versions of Oracle
      if (r != 1 && r != Statement.SUCCESS_NO_INFO) {
        throw new DatabaseException("Batch did not return the expected result: " + Arrays.toString(result));
      }
    }
  }

  @Override
  public int[] insertBatchUnchecked() {
    return updateBatch();
  }

  @Override
  public Long insertReturningPkSeq(String primaryKeyColumnName) {
    if (!hasPk()) {
      throw new DatabaseException("Call argPkSeq() before insertReturningPkSeq()");
    }

    if (options.flavor().supportsInsertReturning()) {
      return updateInternal(1, primaryKeyColumnName);
    } else {
      // Simulate by issuing a select for the next sequence value, inserting, and returning it
      Long pk = new SqlSelectImpl(connection, mock, options.flavor().sequenceSelectNextVal(pkSeqName), options).queryLongOrNull();
      if (pk == null) {
        throw new DatabaseException("Unable to retrieve next sequence value from " + pkSeqName);
      }
      if (pkArgName != null) {
        namedArg(pkArgName, adaptor.nullNumeric(pk));
      } else {
        parameterList.set(pkPos, adaptor.nullNumeric(pk));
      }
      updateInternal(1);
      return pk;
    }
  }

  @Override
  public <T> T insertReturning(String tableName, String primaryKeyColumnName, RowsHandler<T> handler,
                               String... otherColumnNames) {
    if (!hasPk()) {
      throw new DatabaseException("Identify a primary key with argPk*() before insertReturning()");
    }

    if (options.flavor().supportsInsertReturning()) {
      return updateInternal(1, primaryKeyColumnName, handler, otherColumnNames);
    } else if (pkSeqName != null) {
      // Simulate by issuing a select for the next sequence value, inserting, and returning it
      Long pk = new SqlSelectImpl(connection, mock, options.flavor().sequenceSelectNextVal(pkSeqName), options)
          .queryLongOrNull();
      if (pk == null) {
        throw new DatabaseException("Unable to retrieve next sequence value from " + pkSeqName);
      }
      if (pkArgName != null) {
        namedArg(pkArgName, adaptor.nullNumeric(pk));
      } else {
        parameterList.set(pkPos, adaptor.nullNumeric(pk));
      }
      updateInternal(1);
      StringBuilder sql = new StringBuilder();
      sql.append("select ").append(primaryKeyColumnName);
      for (String colName : otherColumnNames) {
        sql.append(", ").append(colName);
      }
      sql.append(" from ").append(tableName).append(" where ").append(primaryKeyColumnName).append("=?");
      return new SqlSelectImpl(connection, mock, sql.toString(), options).argLong(pk).query(handler);
    } else if (pkLong != null) {
      // Insert the value, then do a select based on the primary key
      updateInternal(1);
      StringBuilder sql = new StringBuilder();
      sql.append("select ").append(primaryKeyColumnName);
      for (String colName : otherColumnNames) {
        sql.append(", ").append(colName);
      }
      sql.append(" from ").append(tableName).append(" where ").append(primaryKeyColumnName).append("=?");
      return new SqlSelectImpl(connection, mock, sql.toString(), options).argLong(pkLong).query(handler);
    } else {
      // Should never happen if our safety checks worked
      throw new DatabaseException("Internal error");
    }
  }

  @Nonnull
  @Override
  public SqlInsert argPkSeq(@Nonnull String sequenceName) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkSeqName = sequenceName;
    SqlInsert sqlInsert = positionalArg(new RewriteArg(options.flavor().sequenceNextVal(sequenceName)));
    pkPos = parameterList.size() - 1;
    return sqlInsert;
  }

  @Override
  @Nonnull
  public SqlInsert argPkSeq(@Nonnull String argName, @Nonnull String sequenceName) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkArgName = argName;
    pkSeqName = sequenceName;
    return namedArg(argName, new RewriteArg(options.flavor().sequenceNextVal(sequenceName)));
  }

  @Override
  @Nonnull
  public SqlInsert argPkLong(String argName, Long arg) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkLong = arg;
    return namedArg(argName, adaptor.nullNumeric(arg));
  }

  @Override
  @Nonnull
  public SqlInsert argPkLong(Long arg) {
    if (hasPk()) {
      throw new DatabaseException("Only call one argPk*() method");
    }
    pkLong = arg;
    return positionalArg(adaptor.nullNumeric(arg));
  }

  private boolean hasPk() {
    return pkArgName != null || pkSeqName != null || pkLong != null;
  }

  private int[] updateBatch() {
    batch();

    if (batched == null || batched.size() == 0) {
      throw new DatabaseException("Batch insert requires parameters");
    }

    PreparedStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql = sql;
    Object[] firstRowParameters = null;
    List<Object[]> parameters = new ArrayList<>();

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      for (Batch batch : batched) {
        MixedParameterSql mpSql = new MixedParameterSql(sql, batch.parameterList, batch.parameterMap);
        if (firstRowParameters == null) {
          executeSql = mpSql.getSqlToExecute();
          firstRowParameters = mpSql.getArgs();
        } else {
          if (!executeSql.equals(mpSql.getSqlToExecute())) {
            throw new DatabaseException("All rows in a batch must use parameters in the same way. \nSQL1: "
                + executeSql + "\nSQL2: " + mpSql.getSqlToExecute());
          }
        }
        parameters.add(mpSql.getArgs());
      }

      if (connection != null) {
        ps = connection.prepareStatement(executeSql);

        for (Object[] params : parameters) {
          adaptor.addParameters(ps, params);
          ps.addBatch();
        }

        metric.checkpoint("prep");
        int[] numAffectedRows = ps.executeBatch();
        metric.checkpoint("execBatch[" + parameters.size() + "]");
        isSuccess = true;
        return numAffectedRows;
      } else {
        int[] result = new int[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
          Object[] params = parameters.get(i);
          Integer numAffectedRows = mock.insert(executeSql, DebugSql.printDebugOnlySqlString(executeSql, params, options));
          if (numAffectedRows == null) {
            // No mock behavior provided, be nice and assume the expected value
            log.debug("Setting numAffectedRows to expected");
            numAffectedRows = 1;
          }
          result[i] = numAffectedRows;
        }
        metric.checkpoint("stubBatch[" + parameters.size() + "]");
        isSuccess = true;
        return result;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, firstRowParameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, firstRowParameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, firstRowParameters, options, logEx);
      }
    }
  }

  private int updateInternal(int expectedNumAffectedRows) {
    if (batched != null) {
      throw new DatabaseException("Call insertBatch() if you are using the batch() feature");
    }

    PreparedStatement ps = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql = sql;
    Object[] parameters = null;

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      MixedParameterSql mpSql = new MixedParameterSql(sql, parameterList, parameterMap);
      executeSql = mpSql.getSqlToExecute();
      parameters = mpSql.getArgs();

      if (connection != null) {
        ps = connection.prepareStatement(executeSql);

        adaptor.addParameters(ps, parameters);
        metric.checkpoint("prep");
        int numAffectedRows = ps.executeUpdate();
        metric.checkpoint("exec[" + numAffectedRows + "]");
        if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
          errorCode = options.generateErrorCode();
          throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
              + expectedNumAffectedRows + " were expected." + "\n"
              + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
        }
        isSuccess = true;
        return numAffectedRows;
      } else {
        Integer numAffectedRows = mock.insert(executeSql, DebugSql.printDebugOnlySqlString(executeSql, parameters, options));
        if (numAffectedRows == null) {
          // No mock behavior provided, be nice and assume the expected value
          log.debug("Setting numAffectedRows to expected");
          numAffectedRows = expectedNumAffectedRows;
        }
        metric.checkpoint("stub[" + numAffectedRows + "]");
        isSuccess = true;
        return numAffectedRows;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private Long updateInternal(int expectedNumAffectedRows, @Nonnull String pkToReturn) {
    if (batched != null) {
      throw new DatabaseException("Call insertBatch() if you are using the batch() feature");
    }

    PreparedStatement ps = null;
    ResultSet rs = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql = sql;
    Object[] parameters = null;

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      MixedParameterSql mpSql = new MixedParameterSql(sql, parameterList, parameterMap);
      executeSql = mpSql.getSqlToExecute();
      parameters = mpSql.getArgs();

      if (connection != null) {
        ps = connection.prepareStatement(executeSql, new String[] { pkToReturn });

        adaptor.addParameters(ps, parameters);
        metric.checkpoint("prep");
        int numAffectedRows = ps.executeUpdate();
        metric.checkpoint("exec[" + numAffectedRows + "]");
        if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
          errorCode = options.generateErrorCode();
          throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
              + expectedNumAffectedRows + " were expected." + "\n"
              + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
        }
        rs = ps.getGeneratedKeys();
        Long pk = null;
        if (rs != null && rs.next()) {
          pk = rs.getLong(1);
        }
        isSuccess = true;
        return pk;
      } else {
        String debugSql = DebugSql.printDebugOnlySqlString(executeSql, parameters, options);
        Long pk = mock.insertReturningPk(executeSql, debugSql);
        if (pk == null) {
          // No mock behavior provided, default to something that could conceivably work
          log.debug("Setting pk to hash of debugSql");
          pk = (long) debugSql.hashCode();
        }
        metric.checkpoint("stub");
        isSuccess = true;
        return pk;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(rs, log);
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private <T> T updateInternal(int expectedNumAffectedRows, @Nonnull String pkToReturn, RowsHandler<T> handler,
                              String... otherCols) {
    if (batched != null) {
      throw new DatabaseException("Call insertBatch() if you are using the batch() feature");
    }

    PreparedStatement ps = null;
    ResultSet rs = null;
    Metric metric = new Metric(log.isDebugEnabled());

    String executeSql = sql;
    Object[] parameters = null;

    boolean isSuccess = false;
    String errorCode = null;
    Exception logEx = null;
    try {
      MixedParameterSql mpSql = new MixedParameterSql(sql, parameterList, parameterMap);
      executeSql = mpSql.getSqlToExecute();
      parameters = mpSql.getArgs();

      String[] returnCols = new String[otherCols.length + 1];
      returnCols[0] = pkToReturn;
      System.arraycopy(otherCols, 0, returnCols, 1, otherCols.length);

      if (connection != null) {
        ps = connection.prepareStatement(executeSql, returnCols);

        adaptor.addParameters(ps, parameters);
        metric.checkpoint("prep");
        int numAffectedRows = ps.executeUpdate();
        metric.checkpoint("exec[" + numAffectedRows + "]");
        if (expectedNumAffectedRows > 0 && numAffectedRows != expectedNumAffectedRows) {
          errorCode = options.generateErrorCode();
          throw new WrongNumberOfRowsException("The number of affected rows was " + numAffectedRows + ", but "
              + expectedNumAffectedRows + " were expected." + "\n"
              + DebugSql.exceptionMessage(executeSql, parameters, errorCode, options));
        }
        rs = ps.getGeneratedKeys();
        final ResultSet finalRs = rs;
        T result = handler.process(new RowsAdaptor(finalRs, options));
        metric.checkpoint("read");
        isSuccess = true;
        return result;
      } else {
        RowStub stub = mock.insertReturning(executeSql, DebugSql.printDebugOnlySqlString(executeSql, parameters, options));
        if (stub == null) {
          stub = new RowStub();
        }
        metric.checkpoint("stub");
        T result = handler.process(stub.toRows());
        metric.checkpoint("read");
        isSuccess = true;
        return result;
      }
    } catch (WrongNumberOfRowsException e) {
      throw e;
    } catch (Exception e) {
      errorCode = options.generateErrorCode();
      logEx = e;
      throw DatabaseException.wrap(DebugSql.exceptionMessage(executeSql, parameters, errorCode, options), e);
    } finally {
      adaptor.closeQuietly(rs, log);
      adaptor.closeQuietly(ps, log);
      metric.done("close");
      if (isSuccess) {
        DebugSql.logSuccess("Insert", log, metric, executeSql, parameters, options);
      } else {
        DebugSql.logError("Insert", log, metric, errorCode, executeSql, parameters, options, logEx);
      }
    }
  }

  private SqlInsert positionalArg(Object arg) {
    if (parameterList == null) {
      parameterList = new ArrayList<>();
    }
    parameterList.add(arg);
    return this;
  }

  private SqlInsert namedArg(String argName, Object arg) {
    if (parameterMap == null) {
      parameterMap = new HashMap<>();
    }
    if (argName.startsWith(":")) {
      argName = argName.substring(1);
    }
    parameterMap.put(argName, arg);
    return this;
  }

  private String booleanToString(Boolean b) {
    return b == null ? null : b ? "Y" : "N";
  }

  private class Batch {
    private List<Object> parameterList;       // !null ==> traditional ? args
    private Map<String, Object> parameterMap; // !null ==> named :abc args

    public Batch(List<Object> parameterList, Map<String, Object> parameterMap) {
      this.parameterList = parameterList;
      this.parameterMap = parameterMap;
    }
  }
}
