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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Enumeration of supported databases with various compatibility settings.
 *
 * @author garricko
 */
public enum Flavor {
  derby {
    private TimeZone gmt = TimeZone.getTimeZone("GMT");

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "next value for " + sequenceName;
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "values next value for " + sequenceName;
    }

    @Override
    public String sequenceDrop(String dbtestSeq) {
      return "drop sequence " + dbtestSeq + " restrict";
    }

    @Override
    public String sequenceCacheClause(int nbrValuesToCache) {
      return "";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return "";
    }

    @Override
    public String dbTimeMillis() {
      return "current_timestamp";
    }

    @Override
    public String fromAny() {
      return " from sysibm.sysdummy1";
    }

    @Override
    public String dateAsSqlFunction(Date date) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setTimeZone(gmt);
      return "timestamp('" + dateFormat.format(date) + "')";
    }
  },
  oracle {
    private TimeZone gmt = TimeZone.getTimeZone("GMT");

    @Override
    public String typeFloat() {
      return "binary_float";
    }

    @Override
    public String typeDouble() {
      return "binary_double";
    }

    @Override
    public String typeInteger() {
      return "numeric(10)";
    }

    @Override
    public String typeLong() {
      return "numeric(19)";
    }

    @Override
    public String typeDate() {
      return "timestamp(3)";
    }

    @Override
    public String typeStringVar(int bytes) {
      return "varchar2(" + bytes + ")";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return order ? " order" : " noorder";
    }

    @Override
    public String sequenceCycleClause(boolean cycle) {
      return cycle ? " cycle" : " nocycle";
    }

    @Override
    public boolean supportsInsertReturning() {
      return true;
    }

    @Override
    public String dbTimeMillis() {
      return "systimestamp(3)";
    }

    @Override
    public String fromAny() {
      return " from dual";
    }

    @Override
    public String dateAsSqlFunction(Date date) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setTimeZone(gmt);
      return "timestamp '" + dateFormat.format(date) + "'";
    }
  },
  postgresql {
    private TimeZone gmt = TimeZone.getTimeZone("GMT");

    @Override
    public String typeDouble() {
      return "double precision";
    }

    @Override
    public String typeClob() {
      return "text";
    }

    @Override
    public String typeBlob() {
      return "bytea";
    }

    @Override
    public String typeDate() {
      return "timestamp(3)";
    }

    @Override
    public boolean useStringForClob() {
      return true;
    }

    @Override
    public boolean useBytesForBlob() {
      return true;
    }

    @Override
    public String sequenceNextVal(String sequenceName) {
      return "nextval('" + sequenceName + "')";
    }

    @Override
    public String sequenceSelectNextVal(String sequenceName) {
      return "select nextval('" + sequenceName + "')";
    }

    @Override
    public String sequenceOrderClause(boolean order) {
      return "";
    }

    @Override
    public boolean supportsInsertReturning() {
      return true;
    }

    @Override
    public String dbTimeMillis() {
      return "date_trunc('milliseconds',localtimestamp)";
    }

    @Override
    public String dateAsSqlFunction(Date date) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
      dateFormat.setTimeZone(gmt);
      return "'" + dateFormat.format(date) + " GMT'::timestamp";
    }
  };

  public String typeInteger() {
    return "integer";
  }

  public String typeBoolean() {
    return "char(1)";
  }

  public String typeLong() {
    return "bigint";
  }

  public String typeFloat() {
    return "real";
  }

  public String typeDouble() {
    return "double";
  }

  public String typeBigDecimal(int size, int precision) {
    return "numeric(" + size + "," + precision + ")";
  }

  public String typeStringVar(int bytes) {
    return "varchar(" + bytes + ")";
  }

  public String typeStringFixed(int bytes) {
    return "char(" + bytes + ")";
  }

  public String typeClob() {
    return "clob";
  }

  public String typeBlob() {
    return "blob";
  }

  public String typeDate() {
    return "timestamp";
  }

  public boolean useStringForClob() {
    return false;
  }

  public boolean useBytesForBlob() {
    return false;
  }

  public String sequenceNextVal(String sequenceName) {
    return sequenceName + ".nextval";
  }

  public String sequenceSelectNextVal(String sequenceName) {
    return "select " + sequenceName + ".nextval from dual";
  }

  public String sequenceDrop(String dbtestSeq) {
    return "drop sequence " + dbtestSeq;
  }

  public boolean supportsInsertReturning() {
    return false;
  }

  public String dbTimeMillis() {
    return "current_time";
  }

  public static Flavor fromJdbcUrl(String url) {
    if (url.startsWith("jdbc:postgresql:")) {
      return postgresql;
    } else if (url.startsWith("jdbc:oracle:")) {
      return oracle;
    } else if (url.startsWith("jdbc:derby:")) {
      return derby;
    } else {
      throw new DatabaseException("Cannot determine database flavor from url");
    }
  }

  public String sequenceCacheClause(int nbrValuesToCache) {
    return " cache " + nbrValuesToCache;
  }

  public String sequenceOrderClause(boolean order) {
    return order ? " order" : " no order";
  }

  public String sequenceCycleClause(boolean cycle) {
    return cycle ? " cycle" : " no cycle";
  }

  /**
   * Indicate what should follow a constant select statement. For example, "select 1"
   * works on some databases, while Oracle requires "select 1 from dual". For Oracle
   * this function should return " from dual" (including the leading space).
   */
  public String fromAny() {
    return "";
  }

  /**
   * Return a SQL function representing the specified date. For example, in PostgreSQL this
   * looks like "'1970-01-02 02:17:36.789000 GMT'::timestamp".
   */
  public abstract String dateAsSqlFunction(Date date);
}
