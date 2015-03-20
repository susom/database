package com.github.susom.database;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Convenience for specifying hard-coded values for the Rows object. Useful for testing,
 * especially with Mock libraries.
 */
public class RowStub {
  private String[] columnNames;
  private List<Object[]> rows = new ArrayList<>();

  public RowStub withColumnNames(String... names) {
    columnNames = names;
    return this;
  }

  public RowStub addRow(Object... columns) {
    rows.add(columns);
    return this;
  }

  public Rows toRows() {
    return new Rows() {
      private int row = -1;
      private int col = -1;

      @Override
      public boolean next() {
        col = -1;
        return !rows.isEmpty() && ++row < rows.size();
      }

      @NotNull
      @Override
      public String[] getColumnNames() {
        requireColumnNames();
        return columnNames;
      }

      @Nullable
      @Override
      public Boolean getBooleanOrNull() {
        return toBoolean(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Boolean getBooleanOrNull(int columnOneBased) {
        return toBoolean(rows.get(row)[columnOneBased - 1]);
      }

      @Nullable
      @Override
      public Boolean getBooleanOrNull(String columnName) {
        return toBoolean(rows.get(row)[columnIndexByName(columnName)]);
      }

      @Override
      public boolean getBooleanOrFalse() {
        Boolean i = getBooleanOrNull();
        return i == null ? false : i;
      }

      @Override
      public boolean getBooleanOrFalse(int columnOneBased) {
        Boolean i = getBooleanOrNull(columnOneBased);
        return i == null ? false : i;
      }

      @Override
      public boolean getBooleanOrFalse(String columnName) {
        Boolean i = getBooleanOrNull(columnName);
        return i == null ? false : i;
      }

      @Override
      public boolean getBooleanOrTrue() {
        Boolean i = getBooleanOrNull();
        return i == null ? true : i;
      }

      @Override
      public boolean getBooleanOrTrue(int columnOneBased) {
        Boolean i = getBooleanOrNull(columnOneBased);
        return i == null ? true : i;
      }

      @Override
      public boolean getBooleanOrTrue(String columnName) {
        Boolean i = getBooleanOrNull(columnName);
        return i == null ? true : i;
      }

      @Nullable
      @Override
      public Integer getIntegerOrNull() {
        return toInteger(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Integer getIntegerOrNull(int columnOneBased) {
        return toInteger(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public Integer getIntegerOrNull(String columnName) {
        return toInteger(rows.get(row)[columnIndexByName(columnName)]);
      }

      @Override
      public int getIntegerOrZero() {
        Integer i = getIntegerOrNull();
        return i == null ? 0 : i;
      }

      @Override
      public int getIntegerOrZero(int columnOneBased) {
        Integer i = getIntegerOrNull(columnOneBased);
        return i == null ? 0 : i;
      }

      @Override
      public int getIntegerOrZero(String columnName) {
        Integer i = getIntegerOrNull(columnName);
        return i == null ? 0 : i;
      }

      @Nullable
      @Override
      public Long getLongOrNull() {
        return toLong(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Long getLongOrNull(int columnOneBased) {
        return toLong(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public Long getLongOrNull(String columnName) {
        return toLong(rows.get(row)[columnIndexByName(columnName)]);
      }

      @Override
      public long getLongOrZero() {
        Long i = getLongOrNull();
        return i == null ? 0 : i;
      }

      @Override
      public long getLongOrZero(int columnOneBased) {
        Long i = getLongOrNull(columnOneBased);
        return i == null ? 0 : i;
      }

      @Override
      public long getLongOrZero(String columnName) {
        Long i = getLongOrNull(columnName);
        return i == null ? 0 : i;
      }

      @Nullable
      @Override
      public Float getFloatOrNull() {
        return toFloat(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Float getFloatOrNull(int columnOneBased) {
        return toFloat(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public Float getFloatOrNull(String columnName) {
        return toFloat(rows.get(row)[columnIndexByName(columnName)]);
      }

      @Override
      public float getFloatOrZero() {
        Float i = getFloatOrNull();
        return i == null ? 0 : i;
      }

      @Override
      public float getFloatOrZero(int columnOneBased) {
        Float i = getFloatOrNull(columnOneBased);
        return i == null ? 0 : i;
      }

      @Override
      public float getFloatOrZero(String columnName) {
        Float i = getFloatOrNull(columnName);
        return i == null ? 0 : i;
      }

      @Nullable
      @Override
      public Double getDoubleOrNull() {
        return toDouble(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Double getDoubleOrNull(int columnOneBased) {
        return toDouble(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public Double getDoubleOrNull(String columnName) {
        return toDouble(rows.get(row)[columnIndexByName(columnName)]);
      }

      @Override
      public double getDoubleOrZero() {
        Double i = getDoubleOrNull();
        return i == null ? 0 : i;
      }

      @Override
      public double getDoubleOrZero(int columnOneBased) {
        Double i = getDoubleOrNull(columnOneBased);
        return i == null ? 0 : i;
      }

      @Override
      public double getDoubleOrZero(String columnName) {
        Double i = getDoubleOrNull(columnName);
        return i == null ? 0 : i;
      }

      @Nullable
      @Override
      public BigDecimal getBigDecimalOrNull() {
        return toBigDecimal(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public BigDecimal getBigDecimalOrNull(int columnOneBased) {
        return toBigDecimal(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public BigDecimal getBigDecimalOrNull(String columnName) {
        return toBigDecimal(rows.get(row)[columnIndexByName(columnName)]);
      }

      @NotNull
      @Override
      public BigDecimal getBigDecimalOrZero() {
        BigDecimal i = getBigDecimalOrNull();
        return i == null ? new BigDecimal(0) : i;
      }

      @NotNull
      @Override
      public BigDecimal getBigDecimalOrZero(int columnOneBased) {
        BigDecimal i = getBigDecimalOrNull(columnOneBased);
        return i == null ? new BigDecimal(0) : i;
      }

      @NotNull
      @Override
      public BigDecimal getBigDecimalOrZero(String columnName) {
        BigDecimal i = getBigDecimalOrNull(columnName);
        return i == null ? new BigDecimal(0) : i;
      }

      @Nullable
      @Override
      public String getStringOrNull() {
        return toString(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public String getStringOrNull(int columnOneBased) {
        return toString(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public String getStringOrNull(String columnName) {
        return toString(rows.get(row)[columnIndexByName(columnName)]);
      }

      @NotNull
      @Override
      public String getStringOrEmpty() {
        String i = getStringOrNull();
        return i == null ? "" : i;
      }

      @NotNull
      @Override
      public String getStringOrEmpty(int columnOneBased) {
        String i = getStringOrNull(columnOneBased);
        return i == null ? "" : i;
      }

      @NotNull
      @Override
      public String getStringOrEmpty(String columnName) {
        String i = getStringOrNull(columnName);
        return i == null ? "" : i;
      }

      @Nullable
      @Override
      public String getClobStringOrNull() {
        return getStringOrNull();
      }

      @Nullable
      @Override
      public String getClobStringOrNull(int columnOneBased) {
        return getStringOrNull(columnOneBased);
      }

      @Nullable
      @Override
      public String getClobStringOrNull(String columnName) {
        return getStringOrNull(columnName);
      }

      @NotNull
      @Override
      public String getClobStringOrEmpty() {
        return getStringOrEmpty();
      }

      @NotNull
      @Override
      public String getClobStringOrEmpty(int columnOneBased) {
        return getStringOrEmpty(columnOneBased);
      }

      @NotNull
      @Override
      public String getClobStringOrEmpty(String columnName) {
        return getStringOrEmpty(columnName);
      }

      @Nullable
      @Override
      public Reader getClobReaderOrNull() {
        String s = getStringOrNull();
        return s == null ? null : new StringReader(s);
      }

      @Nullable
      @Override
      public Reader getClobReaderOrNull(int columnOneBased) {
        String s = getStringOrNull(columnOneBased);
        return s == null ? null : new StringReader(s);
      }

      @Nullable
      @Override
      public Reader getClobReaderOrNull(String columnName) {
        String s = getStringOrNull(columnName);
        return s == null ? null : new StringReader(s);
      }

      @NotNull
      @Override
      public Reader getClobReaderOrEmpty() {
        return new StringReader(getStringOrEmpty());
      }

      @NotNull
      @Override
      public Reader getClobReaderOrEmpty(int columnOneBased) {
        return new StringReader(getStringOrEmpty(columnOneBased));
      }

      @NotNull
      @Override
      public Reader getClobReaderOrEmpty(String columnName) {
        return new StringReader(getStringOrEmpty(columnName));
      }

      @Nullable
      @Override
      public byte[] getBlobBytesOrNull() {
        return toBytes(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public byte[] getBlobBytesOrNull(int columnOneBased) {
        return toBytes(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public byte[] getBlobBytesOrNull(String columnName) {
        return toBytes(rows.get(row)[columnIndexByName(columnName)]);
      }

      @NotNull
      @Override
      public byte[] getBlobBytesOrZeroLen() {
        byte[] a = getBlobBytesOrNull();
        return a == null ? new byte[0] : a;
      }

      @NotNull
      @Override
      public byte[] getBlobBytesOrZeroLen(int columnOneBased) {
        byte[] a = getBlobBytesOrNull(columnOneBased);
        return a == null ? new byte[0] : a;
      }

      @NotNull
      @Override
      public byte[] getBlobBytesOrZeroLen(String columnName) {
        byte[] a = getBlobBytesOrNull(columnName);
        return a == null ? new byte[0] : a;
      }

      @Nullable
      @Override
      public InputStream getBlobInputStreamOrNull() {
        byte[] a = getBlobBytesOrNull();
        return a == null ? null : new ByteArrayInputStream(a);
      }

      @Nullable
      @Override
      public InputStream getBlobInputStreamOrNull(int columnOneBased) {
        byte[] a = getBlobBytesOrNull(columnOneBased);
        return a == null ? null : new ByteArrayInputStream(a);
      }

      @Nullable
      @Override
      public InputStream getBlobInputStreamOrNull(String columnName) {
        byte[] a = getBlobBytesOrNull(columnName);
        return a == null ? null : new ByteArrayInputStream(a);
      }

      @NotNull
      @Override
      public InputStream getBlobInputStreamOrEmpty() {
        return new ByteArrayInputStream(getBlobBytesOrZeroLen());
      }

      @NotNull
      @Override
      public InputStream getBlobInputStreamOrEmpty(int columnOneBased) {
        return new ByteArrayInputStream(getBlobBytesOrZeroLen(columnOneBased));
      }

      @NotNull
      @Override
      public InputStream getBlobInputStreamOrEmpty(String columnName) {
        return new ByteArrayInputStream(getBlobBytesOrZeroLen(columnName));
      }

      @Nullable
      @Override
      public Date getDateOrNull() {
        return toDate(rows.get(row)[++col]);
      }

      @Nullable
      @Override
      public Date getDateOrNull(int columnOneBased) {
        return toDate(rows.get(row)[columnOneBased-1]);
      }

      @Nullable
      @Override
      public Date getDateOrNull(String columnName) {
        return toDate(rows.get(row)[columnIndexByName(columnName)]);
      }

      private void requireColumnNames() {
        if (columnNames == null) {
          throw new DatabaseException("Column names were not provided for this stub");
        }
      }

      private int columnIndexByName(String columnName) {
        requireColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
          if (columnName.equals(columnNames[i])) {
            return i;
          }
        }
        throw new DatabaseException("Column name '" + columnName + "' not found");
      }

      private Boolean toBoolean(Object o) {
        if (o instanceof String) {
          if ("Y".equals(o)) {
            return Boolean.TRUE;
          } else if ("N".equals(o)) {
            return Boolean.FALSE;
          } else {
            throw new DatabaseException("Value returned for boolean was not 'Y' or 'N'");
          }
        }
        return (Boolean) o;
      }

      private Integer toInteger(Object o) {
        return (Integer) o;
      }

      private Long toLong(Object o) {
        if (o instanceof Integer) {
          return ((Integer) o).longValue();
        }
        return (Long) o;
      }

      private Float toFloat(Object o) {
        if (o instanceof Integer) {
          return ((Integer) o).floatValue();
        }
        return (Float) o;
      }

      private Double toDouble(Object o) {
        if (o instanceof Integer) {
          return ((Integer) o).doubleValue();
        }
        if (o instanceof Float) {
          return ((Float) o).doubleValue();
        }
        return (Double) o;
      }

      private BigDecimal toBigDecimal(Object o) {
        if (o instanceof Integer) {
          return new BigDecimal(((Integer) o).doubleValue());
        }
        if (o instanceof Long) {
          return new BigDecimal(((Long) o).doubleValue());
        }
        if (o instanceof Float) {
          return new BigDecimal(((Float) o).doubleValue());
        }
        if (o instanceof Double) {
          return new BigDecimal((Double) o);
        }
        return (BigDecimal) o;
      }

      private byte[] toBytes(Object o) {
        return (byte[]) o;
      }

      private String toString(Object o) {
        return (String) o;
      }

      private Date toDate(Object o) {
        if (o instanceof String) {
          String s = (String) o;
          if (s.length() == "yyyy-MM-dd".length()) {
            try {
              return new SimpleDateFormat("yyyy-MM-dd").parse(s);
            } catch (ParseException e) {
              throw new DatabaseException("Could not parse date as yyyy-MM-dd for " + s);
            }
          }
          if (s.length() == "yyyy-MM-ddThh:mm:ss".length()) {
            try {
              return new SimpleDateFormat("yyyy-MM-ddThh:mm:ss").parse(s);
            } catch (ParseException e) {
              throw new DatabaseException("Could not parse date as yyyy-MM-ddThh:mm:ss for " + s);
            }
          }
          throw new DatabaseException("Didn't understand date string: " + s);
        }
        return (Date) o;
      }
    };
  }
}
