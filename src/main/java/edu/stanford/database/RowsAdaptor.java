package edu.stanford.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Safely wrap a ResultSet and provide access to the data it contains.
 *
 * @author garricko
 */
class RowsAdaptor implements Rows {
  private final ResultSet rs;

  public RowsAdaptor(ResultSet rs) {
    this.rs = rs;
  }

  @Override
  public boolean next() {
    try {
      return rs.next();
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Integer getInteger(int columnOneBased) {
    try {
      return toInteger(rs, columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Integer getInteger(String columnName) {
    try {
      return toInteger(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Long getLong(int column) {
    try {
      return toLong(rs, column);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Long getLong(String columnName) {
    try {
      return toLong(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Float getFloat(int columnOneBased) {
    try {
      return toFloat(rs, columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Float getFloat(String columnName) {
    try {
      return toFloat(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Double getDouble(int columnOneBased) {
    try {
      return toDouble(rs, columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Double getDouble(String columnName) {
    try {
      return toDouble(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnOneBased) {
    try {
      return toBigDecimal(rs, columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public BigDecimal getBigDecimal(String columnName) {
    try {
      return toBigDecimal(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public String getString(int columnOneBased) {
    try {
      return rs.getString(columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public String getString(String columnName) {
    try {
      return rs.getString(columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public String getClobString(int columnOneBased) {
    try {
      return rs.getString(columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public String getClobString(String columnName) {
    try {
      return rs.getString(columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Reader getClobReader(int columnOneBased) {
    try {
      return rs.getCharacterStream(columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Reader getClobReader(String columnName) {
    try {
      return rs.getCharacterStream(columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public byte[] getBlobBytes(int columnOneBased) {
    try {
      return rs.getBytes(columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public byte[] getBlobBytes(String columnName) {
    try {
      return rs.getBytes(columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public InputStream getBlobInputStream(int columnOneBased) {
    try {
      return rs.getBinaryStream(columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public InputStream getBlobInputStream(String columnName) {
    try {
      return rs.getBinaryStream(columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Date getDate(int columnOneBased) {
    try {
      return toDate(rs, columnOneBased);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public Date getDate(String columnName) {
    try {
      return toDate(rs, columnName);
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  /**
   * Make sure the Timestamp will return getTime() accurate to the millisecond
   * (if possible) and truncate away nanoseconds.
   */
  private Date timestampToDate(Timestamp ts) {
    long millis = ts.getTime();
    int nanos = ts.getNanos();
    return new Date(millis / 1000 * 1000 + nanos / 1000);
//    ts.setNanos(0);
//    ts.setTime(millis / 1000 * 1000 + nanos / 1000);
//    return ts;
  }

  private Date toDate(ResultSet rs, int col) throws SQLException {
    Timestamp val = rs.getTimestamp(col);
    return val == null ? null : timestampToDate(val);
  }

  private Date toDate(ResultSet rs, String col) throws SQLException {
    Timestamp val = rs.getTimestamp(col);
    return val == null ? null : timestampToDate(val);
  }

  private Integer toInteger(ResultSet rs, int col) throws SQLException {
    int val = rs.getInt(col);
    return rs.wasNull() ? null : val;
  }

  private Integer toInteger(ResultSet rs, String col) throws SQLException {
    int val = rs.getInt(col);
    return rs.wasNull() ? null : val;
  }

  private Long toLong(ResultSet rs, int col) throws SQLException {
    long val = rs.getLong(col);
    return rs.wasNull() ? null : val;
  }

  private Long toLong(ResultSet rs, String col) throws SQLException {
    long val = rs.getLong(col);
    return rs.wasNull() ? null : val;
  }

  private Float toFloat(ResultSet rs, int col) throws SQLException {
    float val = rs.getFloat(col);
    return rs.wasNull() ? null : val;
  }

  private Float toFloat(ResultSet rs, String col) throws SQLException {
    float val = rs.getFloat(col);
    return rs.wasNull() ? null : val;
  }

  private Double toDouble(ResultSet rs, int col) throws SQLException {
    double val = rs.getDouble(col);
    return rs.wasNull() ? null : val;
  }

  private Double toDouble(ResultSet rs, String col) throws SQLException {
    double val = rs.getDouble(col);
    return rs.wasNull() ? null : val;
  }

  private BigDecimal fixBigDecimal(BigDecimal val) {
    if (val.scale() > 0) {
      val = val.stripTrailingZeros();
      if (val.scale() < 0) {
        val = val.setScale(0);
      }
    }
    return val;
  }

  private BigDecimal toBigDecimal(ResultSet rs, int col) throws SQLException {
    BigDecimal val = rs.getBigDecimal(col);
    return val == null ? null : fixBigDecimal(val);
  }

  private BigDecimal toBigDecimal(ResultSet rs, String col) throws SQLException {
    BigDecimal val = rs.getBigDecimal(col);
    return val == null ? null : fixBigDecimal(val);
  }
}
