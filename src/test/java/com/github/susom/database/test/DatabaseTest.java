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

package com.github.susom.database.test;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.log4j.xml.DOMConfigurator;
import org.easymock.IMocksControl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseException;
import com.github.susom.database.DatabaseImpl;
import com.github.susom.database.Flavor;
import com.github.susom.database.OptionsDefault;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.containsString;

/**
 * Unit tests for the Database and Sql implementation classes.
 *
 * @author garricko
 */
@RunWith(JUnit4.class)
public class DatabaseTest {
  static {
    // Initialize logging
    String log4jConfig = new File("log4j.xml").getAbsolutePath();
    DOMConfigurator.configure(log4jConfig);
    org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(DatabaseTest.class);
    log.info("Initialized log4j using file: " + log4jConfig);
  }

  private OptionsDefault options = new OptionsDefault(Flavor.generic) {
    int errors = 0;

    @Override
    public String generateErrorCode() {
      errors++;
      return Integer.toString(errors);
    }
  };

  @Test
  public void staticSqlToLong() throws Exception {
    Connection c = createNiceMock(Connection.class);
    PreparedStatement ps = createNiceMock(PreparedStatement.class);
    ResultSet rs = createNiceMock(ResultSet.class);

    expect(c.prepareStatement("select 1 from dual")).andReturn(ps);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(true);
    expect(rs.getLong(1)).andReturn(1L);
    expect(rs.wasNull()).andReturn(false);

    replay(c, ps, rs);

    assertEquals(new Long(1), new DatabaseImpl(c, options).select("select 1 from dual").queryLong());

    verify(c, ps, rs);
  }

  @Test
  public void when() throws Exception {
    Database db = new DatabaseImpl(createNiceMock(Connection.class), new OptionsDefault(Flavor.oracle));

    assertEquals("oracle", db.when(Flavor.derby, "derby").when(Flavor.oracle, "oracle").otherwise(""));
    assertEquals("", db.when(Flavor.derby, "derby").otherwise(""));
  }

  /**
   * Verify the default options cause exceptions to be thrown when calling commitNow() and
   * rollbackNow().
   */
  @Test
  public void explicitTransactionControlDisabled() {
    Database db = new DatabaseImpl(createNiceMock(Connection.class), new OptionsDefault(Flavor.oracle));

    try {
      db.commitNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertThat(e.getMessage(), containsString("Calls to commitNow() are not allowed"));
    }

    try {
      db.rollbackNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertThat(e.getMessage(), containsString("Calls to rollbackNow() are not allowed"));
    }
  }

  /**
   * Verify custom options with allowTransactionControl() == true cause the commitNow()
   * and rollbackNow() methods to call the underlying methods on the Connection class.
   */
  @Test
  public void explicitTransactionControlEnabled() throws Exception {
    Connection c = createNiceMock(Connection.class);

    c.commit();
    c.rollback();

    replay(c);

    Database db = new DatabaseImpl(c, new OptionsDefault(Flavor.oracle) {
      @Override
      public boolean allowTransactionControl() {
        return true;
      }
    });

    db.commitNow();
    db.rollbackNow();

    verify(c);
  }

  @Test
  public void staticSqlToLongNoRows() throws Exception {
    Connection c = createNiceMock(Connection.class);
    PreparedStatement ps = createNiceMock(PreparedStatement.class);
    ResultSet rs = createNiceMock(ResultSet.class);

    expect(c.prepareStatement("select * from dual")).andReturn(ps);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);

    replay(c, ps, rs);

    assertNull(new DatabaseImpl(c, options).select("select * from dual").queryLong());

    verify(c, ps, rs);
  }

  @Test
  public void staticSqlToLongNullValue() throws Exception {
    Connection c = createNiceMock(Connection.class);
    PreparedStatement ps = createNiceMock(PreparedStatement.class);
    ResultSet rs = createNiceMock(ResultSet.class);

    expect(c.prepareStatement("select null from dual")).andReturn(ps);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(true);
    expect(rs.getLong(1)).andReturn(0L);
    expect(rs.wasNull()).andReturn(true);

    replay(c, ps, rs);

    assertNull(new DatabaseImpl(c, options).select("select null from dual").queryLong());

    verify(c, ps, rs);
  }

  @Test
  public void sqlArgLongPositional() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b where c=?")).andReturn(ps);
    ps.setObject(eq(1), eq(new Long(1)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options).select("select a from b where c=?").argLong(1L).queryLong());

    control.verify();
  }

  @Test
  public void sqlArgLongNull() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b where c=?")).andReturn(ps);
    ps.setNull(eq(1), eq(Types.NUMERIC));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options).select("select a from b where c=?").argLong(null).queryLong());

    control.verify();
  }

  @Test
  public void settingTimeout() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b")).andReturn(ps);
    ps.setQueryTimeout(21);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options).select("select a from b").withTimeoutSeconds(21).queryLong());

    control.verify();
  }

  @Test
  public void settingMaxRows() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b")).andReturn(ps);
    ps.setMaxRows(15);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options).select("select a from b").withMaxRows(15).queryLong());

    control.verify();
  }

  @Test
  public void sqlArgLongNamed() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select ':a' from b where c=?")).andReturn(ps);
    ps.setObject(eq(1), eq(new Long(1)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options).select("select '::a' from b where c=:c").argLong("c", 1L).queryLong());

    control.verify();
  }

  @Test
  public void missingPositionalParameter() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        int errors = 0;

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).select("select a from b where c=?").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1)", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void extraPositionalParameter() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        int errors = 0;

        @Override
        public boolean isDetailedExceptions() {
          return true;
        }

        @Override
        public boolean isLogParameters() {
          return true;
        }

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).select("select a from b where c=?").argString("hi").argInteger(1).queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1): (wrong # args) query: select a from b where c=?", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void missingNamedParameter() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        int errors = 0;

        @Override
        public boolean isDetailedExceptions() {
          return true;
        }

        @Override
        public boolean isLogParameters() {
          return true;
        }

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).select("select a from b where c=:x").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1): select a from b where c=:x", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void missingNamedParameter2() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, options).select("select a from b where c=:x and d=:y").argString("x", "hi").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1)", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void extraNamedParameter() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, options).select("select a from b where c=:x")
          .argString("x", "hi").argString("y", "bye").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1)", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void mixedParameterTypes() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b where c=? and d=?")).andReturn(ps);
    ps.setObject(eq(1), eq("bye"));
    ps.setNull(eq(2), eq(Types.TIMESTAMP));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    new DatabaseImpl(c, options).select("select a from b where c=:x and d=?")
        .argString(":x", "bye").argDate(null).queryLong();

    control.verify();
  }

  @Test
  public void mixedParameterTypesReversed() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b where c=? and d=?")).andReturn(ps);
    ps.setObject(eq(1), eq("bye"));
    ps.setNull(eq(2), eq(Types.TIMESTAMP));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    // Reverse order of args should be the same
    new DatabaseImpl(c, options).select("select a from b where c=:x and d=?")
        .argDate(null).argString(":x", "bye").queryLong();

    control.verify();
  }

  @Test
  public void wrongNumberOfInserts() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);

    expect(c.prepareStatement("insert into x (y) values (1)")).andReturn(ps);
    expect(ps.executeUpdate()).andReturn(2);
    ps.close();

    control.replay();

    try {
      new DatabaseImpl(c, options).insert("insert into x (y) values (1)").insert(1);
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertThat(e.getMessage(), containsString("The number of affected rows was 2, but 1 were expected."));
    }

    control.verify();
  }

  @Test
  public void cancelQuery() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);

    expect(c.prepareStatement("select a from b")).andReturn(ps);
    expect(ps.executeQuery()).andThrow(new SQLException("Cancelled", "cancel", 1013));
    ps.close();

    control.replay();

    try {
      new DatabaseImpl(c, options).select("select a from b").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Timeout of -1 seconds exceeded or user cancelled", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void otherException() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);

    expect(c.prepareStatement("select a from b")).andReturn(ps);
    expect(ps.executeQuery()).andThrow(new RuntimeException("Oops"));
    ps.close();

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        int errors = 0;

        @Override
        public boolean isDetailedExceptions() {
          return true;
        }

        @Override
        public boolean isLogParameters() {
          return true;
        }

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).select("select a from b").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1): select a from b", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void closeExceptions() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement("select a from b")).andReturn(ps);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andThrow(new RuntimeException("Primary"));
    rs.close();
    expectLastCall().andThrow(new RuntimeException("Oops1"));
    ps.close();
    expectLastCall().andThrow(new RuntimeException("Oops2"));

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        int errors = 0;

        @Override
        public boolean isDetailedExceptions() {
          return true;
        }

        @Override
        public boolean isLogParameters() {
          return true;
        }

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).select("select a from b").queryLong();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Error executing SQL (errorCode=1): select a from b", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactionsNotAllowed() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      new DatabaseImpl(c, options).commitNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Calls to commitNow() are not allowed", e.getMessage());
    }

    try {
      new DatabaseImpl(c, options).rollbackNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Calls to rollbackNow() are not allowed", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactionCommitSuccess() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    c.commit();

    control.replay();

    new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
      @Override
      public boolean allowTransactionControl() {
        return true;
      }
    }).commitNow();

    control.verify();
  }

  @Test
  public void transactionCommitFail() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    c.commit();
    expectLastCall().andThrow(new SQLException("Oops"));

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        @Override
        public boolean allowTransactionControl() {
          return true;
        }
      }).commitNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Unable to commit transaction", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactionRollbackSuccess() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    c.rollback();

    control.replay();

    new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
      @Override
      public boolean allowTransactionControl() {
        return true;
      }
    }).rollbackNow();

    control.verify();
  }

  @Test
  public void transactionRollbackFail() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    c.rollback();
    expectLastCall().andThrow(new SQLException("Oops"));

    control.replay();

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.generic) {
        @Override
        public boolean allowTransactionControl() {
          return true;
        }
      }).rollbackNow();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Unable to rollback transaction", e.getMessage());
    }

    control.verify();
  }
}
