package oracle.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

// Fake interface so we can compile without the Oracle JDBC driver
public interface OraclePreparedStatement extends PreparedStatement {
  void setBinaryFloat(int var1, float var2) throws SQLException;
  void setBinaryDouble(int var1, double var2) throws SQLException;
}
