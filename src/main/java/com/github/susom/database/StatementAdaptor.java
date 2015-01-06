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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Scanner;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import oracle.jdbc.OraclePreparedStatement;

/**
 * Deal with mapping parameters into prepared statements.
 *
 * @author garricko
 */
public class StatementAdaptor {
  private final Options options;

  public StatementAdaptor(Options options) {
    this.options = options;
  }

  public void addParameters(PreparedStatement ps, Object[] parameters) throws SQLException {
    for (int i = 0; i < parameters.length; i++) {
      if (parameters[i] == null) {
        ParameterMetaData metaData;
        int parameterType;
        try {
          metaData = ps.getParameterMetaData();
          parameterType = metaData.getParameterType(i + 1);
        } catch (SQLException e) {
          throw new DatabaseException("Parameter " + (i + 1)
              + " was null and the JDBC driver could not report the type of this column."
              + " Please update the JDBC driver to support PreparedStatement.getParameterMetaData()"
              + " or use SqlNull in place of null values to this query.", e);
        }
        ps.setNull(i + 1, parameterType);
      } else if (parameters[i] instanceof SqlNull) {
        SqlNull sqlNull = (SqlNull) parameters[i];
        if (options.useBytesForBlob() && sqlNull.getType() == Types.BLOB) {
          // The setNull() seems more correct, but PostgreSQL chokes on it
          ps.setBytes(i + 1, null);
        } else {
          ps.setNull(i + 1, sqlNull.getType());
        }
      } else if (parameters[i] instanceof Date) {
        // this will correct the millis and nanos according to the JDBC spec
        // if a correct Timestamp is passed in, this will detect that and leave it alone
        ps.setTimestamp(i + 1, toSqlTimestamp((Date) parameters[i]));
      } else if (parameters[i] instanceof Reader) {
        if (options.useStringForClob()) {
          ps.setString(i + 1, readerToString((Reader) parameters[i]));
        } else {
          ps.setCharacterStream(i + 1, (Reader) parameters[i]);
        }
      } else if (parameters[i] instanceof InputStream) {
        if (options.useBytesForBlob()) {
          ps.setBytes(i + 1, streamToBytes((InputStream) parameters[i]));
        } else {
          ps.setBinaryStream(i + 1, (InputStream) parameters[i]);
        }
      } else if (parameters[i] instanceof Float) {
        if (options.flavor() == Flavor.oracle && ps.isWrapperFor(OraclePreparedStatement.class)) {
          // The Oracle 11 driver setDouble() first converts the double to NUMBER, causing underflow
          // for small values so we need to use the proprietary mechanism
          ps.unwrap(OraclePreparedStatement.class).setBinaryFloat(i + 1, (Float) parameters[i]);
        } else {
          ps.setFloat(i + 1, (Float) parameters[i]);
        }
      } else if (parameters[i] instanceof Double) {
        if (options.flavor() == Flavor.oracle && ps.isWrapperFor(OraclePreparedStatement.class)) {
          // The Oracle 11 driver setDouble() first converts the double to NUMBER, causing underflow
          // for small values so we need to use the proprietary mechanism
          ps.unwrap(OraclePreparedStatement.class).setBinaryDouble(i + 1, (Double) parameters[i]);
        } else {
          ps.setDouble(i + 1, (Double) parameters[i]);
        }
      } else {
        ps.setObject(i + 1, parameters[i]);
      }
    }
  }

  private static String readerToString(Reader r) {
    Scanner s = new Scanner(r).useDelimiter("\\A");
    return s.hasNext() ? s.next() : "";
  }

  private static byte[] streamToBytes(InputStream is) throws SQLException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;

    try {
      while ((length = is.read(buffer)) != -1) {
        out.write(buffer, 0, length);
      }
    } catch (IOException e) {
      throw new SQLException("Unable to convert InputStream parameter to bytes", e);
    }

    return out.toByteArray();
  }

  /**
   * Converts the java.util.Date into a java.sql.Timestamp, following the nanos/millis canonicalization
   * required by the spec. If a java.sql.Timestamp is passed in (since it extends java.util.Date),
   * it will be checked and canonicalized only if not already correct.
   */
  private static Timestamp toSqlTimestamp(Date date) {
    long millis = date.getTime();
    int fractionalSecondMillis = (int) (millis % 1000); // guaranteed < 1000

    if (fractionalSecondMillis == 0) { // this means it's already correct by the spec
      if (date instanceof Timestamp) {
        return (Timestamp) date;
      } else {
        return new Timestamp(millis);
      }
    } else { // the millis are invalid and need to be corrected
      int tsNanos = fractionalSecondMillis * 1000000;
      long tsMillis = millis - fractionalSecondMillis;
      Timestamp timestamp = new Timestamp(tsMillis);
      timestamp.setNanos(tsNanos);
      return timestamp;
    }
  }

  class SqlNull {
    int type;

    SqlNull(int type) {
      this.type = type;
    }

    int getType() {
      return type;
    }
  }

  public Object nullDate(Date arg) {
    if (arg == null) {
      return new SqlNull(Types.TIMESTAMP);
    }
    return new Timestamp(arg.getTime());
  }

  public Object nullNumeric(Number arg) {
    if (arg == null) {
      return new SqlNull(Types.NUMERIC);
    }
    return arg;
  }

  public Object nullString(String arg) {
    if (arg == null) {
      return new SqlNull(Types.VARCHAR);
    }
    return arg;
  }

  public Object nullClobReader(Reader arg) {
    if (arg == null) {
      return new SqlNull(Types.VARCHAR);
    }
    return arg;
  }

  public Object nullBytes(byte[] arg) {
    if (arg == null) {
      return new SqlNull(Types.BLOB);
    }
    return arg;
  }

  public Object nullInputStream(InputStream arg) {
    if (arg == null) {
      return new SqlNull(Types.BLOB);
    }
    return arg;
  }

  public void closeQuietly(@Nullable ResultSet rs, @Nullable Logger log) {
    if (rs != null) {
      try {
        rs.close();
      } catch (Exception e) {
        if (log != null) {
          log.warn("Caught exception closing the ResultSet", e);
        }
      }
    }
  }

  public void closeQuietly(@Nullable Statement s, @Nullable Logger log) {
    if (s != null) {
      try {
        s.close();
      } catch (Exception e) {
        if (log != null) {
          log.warn("Caught exception closing the Statement", e);
        }
      }
    }
  }
}
