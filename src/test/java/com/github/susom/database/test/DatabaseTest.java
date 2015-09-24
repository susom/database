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
import java.util.ArrayList;
import java.util.List;

import javax.inject.Provider;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.xml.DOMConfigurator;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.github.susom.database.Database;
import com.github.susom.database.DatabaseException;
import com.github.susom.database.DatabaseImpl;
import com.github.susom.database.DatabaseMock;
import com.github.susom.database.DatabaseProvider;
import com.github.susom.database.DbRun;
import com.github.susom.database.DebugSql;
import com.github.susom.database.Flavor;
import com.github.susom.database.OptionsDefault;
import com.github.susom.database.OptionsOverride;

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

  private OptionsDefault options = new OptionsDefault(Flavor.postgresql) {
    int errors = 0;

    @Override
    public String generateErrorCode() {
      errors++;
      return Integer.toString(errors);
    }
  };
  private OptionsOverride optionsFullLog = new OptionsOverride(options) {
    @Override
    public boolean isLogParameters() {
      return true;
    }
  };
  private LogCaptureAppender capturedLog;

  @Before
  public void initLogCapture() {
    capturedLog = new LogCaptureAppender();
    capturedLog.setThreshold(Level.DEBUG);
    LogManager.getRootLogger().addAppender(capturedLog);
  }

  @After
  public void stopLogCapture() {
    LogManager.getRootLogger().removeAppender(capturedLog);
  }

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

    assertEquals(new Long(1), new DatabaseImpl(c, options).toSelect("select 1 from dual").queryLongOrNull());

    verify(c, ps, rs);
  }

  @Test
  public void when() throws Exception {
    Database db = new DatabaseImpl(createNiceMock(Connection.class), new OptionsDefault(Flavor.oracle));

    assertEquals("oracle", "" + db.when().oracle("oracle"));
    assertEquals("oracle", db.when().oracle("oracle").other(""));
    assertEquals("oracle", db.when().derby("derby").oracle("oracle").other("other"));
    assertEquals("", db.when().derby("derby").other(""));
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
  public void underlyingConnection() {
    Connection c = createNiceMock(Connection.class);

    try {
      new DatabaseImpl(c, new OptionsDefault(Flavor.derby)).underlyingConnection();
      fail("Should have thrown an exception");
    } catch (DatabaseException e) {
      assertEquals("Calls to underlyingConnection() are not allowed", e.getMessage());
    }

    assertTrue(c == new DatabaseImpl(c, new OptionsOverride() {
      @Override
      public boolean allowConnectionAccess() {
        return true;
      }
    }).underlyingConnection());
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

    assertNull(new DatabaseImpl(c, options).toSelect("select * from dual").queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select null from dual").queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select a from b where c=?").argLong(1L).queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select a from b where c=?").argLong(null).queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select a from b").withTimeoutSeconds(21).queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select a from b").withMaxRows(15).queryLongOrNull());

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

    assertNull(new DatabaseImpl(c, options).toSelect("select '::a' from b where c=:c").argLong("c", 1L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void logFormatNoDebugSql() throws Exception {
    new DatabaseImpl(createNiceMock(DatabaseMock.class), options)
        .toSelect("select a from b where c=?")
        .argInteger(1)
        .queryLongOrNull();

    capturedLog.assertNoWarningsOrErrors();
    assertTrue(capturedLog.messages().get(0).endsWith("\tselect a from b where c=?"));
  }

  @Test
  public void logFormatDebugSqlInteger() throws Exception {
    new DatabaseImpl(createNiceMock(DatabaseMock.class), optionsFullLog)
        .toSelect("select a from b where c=?")
        .argInteger(1)
        .queryLongOrNull();

    capturedLog.assertNoWarningsOrErrors();
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect a from b where c=?${sep}select a from b where c=1");
  }

  @Test
  public void missingPositionalParameter() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);

    control.replay();

    try {
      Long value = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
        int errors = 0;

        @Override
        public String generateErrorCode() {
          errors++;
          return Integer.toString(errors);
        }
      }).toSelect("select a from b where c=?").queryLongOrNull();
      fail("Should have thrown an exception, but returned " + value);
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
      Long value = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      }).toSelect("select a from b where c=?").argString("hi").argInteger(1).queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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
      Long value = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      }).toSelect("select a from b where c=:x").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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
      Long value = new DatabaseImpl(c, options).toSelect("select a from b where c=:x and d=:y")
          .argString("x", "hi").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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
      Long value = new DatabaseImpl(c, options).toSelect("select a from b where c=:x")
          .argString("x", "hi").argString("y", "bye").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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

    assertNull(new DatabaseImpl(c, options).toSelect("select a from b where c=:x and d=?")
        .argString(":x", "bye").argDate(null).queryLongOrNull());

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
    assertNull(new DatabaseImpl(c, options).toSelect("select a from b where c=:x and d=?")
        .argDate(null).argString(":x", "bye").queryLongOrNull());

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
      new DatabaseImpl(c, options).toInsert("insert into x (y) values (1)").insert(1);
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
      Long value = new DatabaseImpl(c, options).toSelect("select a from b").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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
      Long value = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      }).toSelect("select a from b").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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
      Long value = new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      }).toSelect("select a from b").queryLongOrNull();
      fail("Should have thrown an exception but returned " + value);
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

    new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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

    new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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
      new DatabaseImpl(c, new OptionsDefault(Flavor.postgresql) {
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

  @Test
  public void transactCommitOnlyWithNoError() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.commit();
    c.close();

    control.replay();

    new DatabaseProvider(new Provider<Connection>() {
      @Override
      public Connection get() {
        return c;
      }
    }, new OptionsDefault(Flavor.postgresql)).transactCommitOnly(new DbRun() {
      @Override
      public void run(Provider<Database> db) throws Exception {
        db.get();
      }
    });

    control.verify();
  }


  @Test
  public void transactCommitOnlyWithError() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.commit();
    c.close();

    control.replay();

    try {
      new DatabaseProvider(new Provider<Connection>() {
        @Override
        public Connection get() {
          return c;
        }
      }, new OptionsDefault(Flavor.postgresql)).transactCommitOnly(new DbRun() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get();
          throw new Error("Oops");
        }
      });
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Error during transaction", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactCommitOnlyOverrideWithError() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.rollback();
    c.close();

    control.replay();

    try {
      new DatabaseProvider(new Provider<Connection>() {
        @Override
        public Connection get() {
          return c;
        }
      }, new OptionsDefault(Flavor.postgresql)).transactCommitOnly(new DbRun() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get();
          setRollbackOnError(true);
          throw new DatabaseException("Oops");
        }
      });
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Oops", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactCommitOnlyOverrideWithError2() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.rollback();
    c.close();

    control.replay();

    try {
      new DatabaseProvider(new Provider<Connection>() {
        @Override
        public Connection get() {
          return c;
        }
      }, new OptionsDefault(Flavor.postgresql)).transactCommitOnly(new DbRun() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get();
          setRollbackOnly(true);
          throw new DatabaseException("Oops");
        }
      });
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Oops", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactRollbackOnly() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.rollback();
    c.close();

    control.replay();

    new DatabaseProvider(new Provider<Connection>() {
      @Override
      public Connection get() {
        return c;
      }
    }, new OptionsDefault(Flavor.postgresql)).transactRollbackOnly(new DbRun() {
      @Override
      public void run(Provider<Database> db) throws Exception {
        db.get();
      }
    });

    control.verify();
  }

  @Test
  public void transactRollbackOnErrorWithError() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.rollback();
    c.close();

    control.replay();

    try {
      new DatabaseProvider(new Provider<Connection>() {
        @Override
        public Connection get() {
          return c;
        }
      }, new OptionsDefault(Flavor.postgresql)).transactRollbackOnError(new DbRun() {
        @Override
        public void run(Provider<Database> db) throws Exception {
          db.get();
          throw new Exception("Oops");
        }
      });
      fail("Should have thrown an exception");
    } catch (Exception e) {
      assertEquals("Error during transaction", e.getMessage());
    }

    control.verify();
  }

  @Test
  public void transactRollbackOnErrorWithNoError() throws Exception {
    IMocksControl control = createStrictControl();

    final Connection c = control.createMock(Connection.class);

    c.setAutoCommit(false);
    c.commit();
    c.close();

    control.replay();

    new DatabaseProvider(new Provider<Connection>() {
      @Override
      public Connection get() {
        return c;
      }
    }, new OptionsDefault(Flavor.postgresql)).transactRollbackOnError(new DbRun() {
      @Override
      public void run(Provider<Database> db) throws Exception {
        db.get();
      }
    });

    control.verify();
  }

  private static class LogCaptureAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();
    private int warnings = 0;
    private int errors = 0;

    protected void append(LoggingEvent event) {
      if (Level.TRACE.isGreaterOrEqual(event.getLevel())) {
        return;
      }
      synchronized (events) {
        if (event.getLevel().equals(Level.WARN)) {
          warnings++;
        } else if (event.getLevel().isGreaterOrEqual(Level.ERROR)) {
          errors++;
        }
        events.add(event);
      }
    }

    public int nbrWarnings() {
      return warnings;
    }

    public int nbrErrors() {
      return errors;
    }

    public void close() {
      // Nothing to do
    }

    public String toString() {
      PatternLayout layout = new PatternLayout("%-5p %c %m\u00AE%n");
      StringBuilder builder = new StringBuilder();
      for (LoggingEvent event : events) {
        builder.append(layout.format(event));
      }
      return builder.toString();
    }

    public List<String> messages() {
      List<String> messages = new ArrayList<>();
      PatternLayout layout = new PatternLayout("%-5p %c %m");
      for (LoggingEvent event : events) {
        messages.add(layout.format(event));
      }
      return messages;
    }

    public void assertNoWarningsOrErrors() {
      assertEquals("Warnings or errors in the log:\n" + toString(), 0, nbrWarnings() + nbrErrors());
    }

    public void assertWarnings(int nbrWarnings) {
      assertEquals("Wrong number of warnings in the log:\n" + toString(), nbrWarnings, nbrWarnings());
    }

    public void assertErrors(int nbrErrors) {
      assertEquals("Wrong number of errors or above in the log:\n" + toString(), nbrErrors, nbrErrors());
    }

    public void assertEntries(int nbrEntries) {
      assertEquals("Wrong number of entries in the log:\n" + toString(), nbrEntries, events.size());
    }

    /**
     * Check for a log entry with a particular log level and message.
     *
     * <p>The message pattern is a literal, exactly matching string,
     * but it may contain a few special tokens that will match varying
     * input in the messages:</p>
     *
     * <ul>
     *   <li>${timing} - this will match a typical section of metrics</li>
     *   <li>${sep} - this will match the boundary between the raw SQL and
     *                the debug sql that logs the parameters as well</li>
     * </ul>
     *
     * @param level the specific level we are looking for
     * @param messagePattern a search pattern (see notes above)
     */
    public void assertMessage(Level level, String messagePattern) {
      boolean found = false;
      for (LoggingEvent event : events) {
        if (!event.getLevel().equals(level)) {
          continue;
        }

        String message = event.getRenderedMessage();
        int messagePos = 0;
        int patternPos = 0;
        while (messagePos < message.length() && patternPos < messagePattern.length()) {
          // Advance until we find a character that doesn't match
          if (message.charAt(messagePos) == messagePattern.charAt(patternPos)) {
            messagePos++;
            patternPos++;
            continue;
          }

          // If message has literal '$' terminate because the pattern doesn't have a special matcher
          if (message.charAt(messagePos) == '$') {
            break;
          }

          // Special matchers: expressions for things that vary with each run
          if (messagePattern.startsWith("${timing}", patternPos) && message.indexOf(')', messagePos) != -1) {
            messagePos = message.indexOf(')', messagePos) + 1;
            patternPos += "${timing}".length();
            continue;
          }
          if (messagePattern.startsWith("${sep}", patternPos)
              && message.startsWith(DebugSql.PARAM_SQL_SEPARATOR, messagePos)) {
            messagePos += DebugSql.PARAM_SQL_SEPARATOR.length();
            patternPos += "${sep}".length();
            continue;
          }

          // Couldn't match
          break;
        }

        if (messagePos >= message.length() && patternPos >= messagePattern.length()) {
          found = true;
          break;
        }
      }
      assertTrue("Log message not found (" + level + " " + messagePattern + ") in log:\n" + toString(), found);
    }

    public boolean requiresLayout() {
      return false;
    }
  }
}
