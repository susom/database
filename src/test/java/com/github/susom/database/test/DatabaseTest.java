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
import com.github.susom.database.DebugSql;
import com.github.susom.database.Flavor;
import com.github.susom.database.Options;
import com.github.susom.database.OptionsDefault;
import com.github.susom.database.OptionsOverride;
import com.github.susom.database.RowStub;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Unit tests for the Database and Sql implementation classes.
 *
 * @author garricko
 */
@RunWith(JUnit4.class)
@SuppressWarnings("ReturnValueIgnored")
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
  private OptionsOverride optionsLegacyParsing = new OptionsOverride(options) {
    @Override
    public boolean useSmartSqlParameterParsing() {
      return false;
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

    assertEquals(Long.valueOf(1), new DatabaseImpl(c, options).toSelect("select 1 from dual").queryLongOrNull());

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
    ps.setObject(eq(1), eq(Long.valueOf(1)));
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
    ps.setObject(eq(1), eq(Long.valueOf(1)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    // With smart parsing, a ':' inside a string literal is left alone and does not need escaping
    assertNull(new DatabaseImpl(c, options).toSelect("select ':a' from b where c=:c").argLong("c", 1L).queryLongOrNull());

    control.verify();
  }

  /**
   * Assert that {@code inputSql} (which uses no bind parameters) is rewritten to
   * {@code expectedSql} when parsed with the given options.
   */
  private void assertParsedSqlNoArgs(Options opts, String inputSql, String expectedSql) throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    expect(c.prepareStatement(expectedSql)).andReturn(ps);
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, opts).toSelect(inputSql).queryLongOrNull());

    control.verify();
  }

  @Test
  public void smartParsingIgnoresCharsInStringLiterals() throws Exception {
    // A '?' or ':' inside a string literal is not a bind variable and needs no escaping
    assertParsedSqlNoArgs(options, "select 'a?b:c' from dual", "select 'a?b:c' from dual");
    // A doubled quote inside the literal does not prematurely end it
    assertParsedSqlNoArgs(options, "select 'it''s a ? and :x' from dual", "select 'it''s a ? and :x' from dual");
    // An unterminated literal is copied through verbatim (no parameters found)
    assertParsedSqlNoArgs(options, "select 'a?b:c from dual", "select 'a?b:c from dual");
  }

  @Test
  public void smartParsingPreservesPostgresCast() throws Exception {
    // The PostgreSQL cast operator (::type) is left untouched, not collapsed or treated as a parameter
    assertParsedSqlNoArgs(options, "select a::text from b", "select a::text from b");
  }

  @Test
  public void smartParsingLeavesLoneColonAlone() throws Exception {
    // A ':' that is not followed by an identifier character is not a named parameter
    assertParsedSqlNoArgs(options, "select a : b from dual", "select a : b from dual");
    // ...including a ':' at the very end of the SQL (boundary condition)
    assertParsedSqlNoArgs(options, "select a from dual :", "select a from dual :");
  }

  @Test
  public void smartParsingDoesNotTreatOperatorsAsComments() throws Exception {
    // A single '-' (subtraction) or '/' (division) is not the start of a comment
    assertParsedSqlNoArgs(options, "select a-b, c/d from e", "select a-b, c/d from e");
  }

  @Test
  public void smartParsingIgnoresCharsInQuotedIdentifier() throws Exception {
    // A '?' or ':' inside a double-quoted identifier is not a bind variable
    assertParsedSqlNoArgs(options, "select x as \"a:b?c\" from b", "select x as \"a:b?c\" from b");
    // A doubled double-quote inside the identifier does not prematurely end it
    assertParsedSqlNoArgs(options, "select x as \"a\"\"b?:c\" from b", "select x as \"a\"\"b?:c\" from b");
  }

  @Test
  public void smartParsingIgnoresCharsInComments() throws Exception {
    // A '?' or ':' inside a line comment or block comment is not a bind variable
    assertParsedSqlNoArgs(options, "select a -- comment with ? and :x\nfrom b",
        "select a -- comment with ? and :x\nfrom b");
    // A line comment that runs to the end of the SQL (no trailing newline)
    assertParsedSqlNoArgs(options, "select a from b -- trailing ? and :x",
        "select a from b -- trailing ? and :x");
    assertParsedSqlNoArgs(options, "select a /* ? and :x */ from b", "select a /* ? and :x */ from b");
    // Nested block comments (as supported by PostgreSQL) are handled
    assertParsedSqlNoArgs(options, "select a /* outer ? /* inner :x */ still ? */ from b",
        "select a /* outer ? /* inner :x */ still ? */ from b");
    // An unterminated block comment is copied through verbatim
    assertParsedSqlNoArgs(options, "select a /* ? and :x from b", "select a /* ? and :x from b");
  }

  @Test
  public void smartParsingFindsParameterAfterComment() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // Parsing resumes after a comment, so a real parameter that follows it is still found
    expect(c.prepareStatement("select a -- pick one: ? or :x\nfrom b where c=?")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(1)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options)
        .toSelect("select a -- pick one: ? or :x\nfrom b where c=:id")
        .argLong("id", 1L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void smartParsingMixesLiteralsAndRealParameters() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // The '?' inside the literal and the '::text' cast are preserved; only the real
    // positional (?) and named (:x) parameters become bind placeholders
    expect(c.prepareStatement("select 'a?b' as r, c::text from b where d=? and e=?")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(1)));
    ps.setObject(eq(2), eq(Long.valueOf(2)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options)
        .toSelect("select 'a?b' as r, c::text from b where d=? and e=:x")
        .argLong(1L).argLong("x", 2L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void smartParsingIgnoresCharsInDollarQuotedStrings() throws Exception {
    // Plain $$ dollar quoting: '?' and ':' inside are not bind variables
    assertParsedSqlNoArgs(options, "select $$?:missing$$ from dual", "select $$?:missing$$ from dual");
    // Tagged dollar quoting: $tag$...$tag$
    assertParsedSqlNoArgs(options, "select $body$? and :x$body$ from dual", "select $body$? and :x$body$ from dual");
    // Content after the dollar-quoted string is still parsed normally
    assertParsedSqlNoArgs(options, "select $$?$$ from dual", "select $$?$$ from dual");
    // Unterminated dollar-quoted string is copied through verbatim
    assertParsedSqlNoArgs(options, "select $$?:missing from dual", "select $$?:missing from dual");
  }

  @Test
  public void smartParsingFindsParameterAfterDollarQuotedString() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // A real parameter after a dollar-quoted string is still found
    expect(c.prepareStatement("select $$?$$ from b where c=?")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(42)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options)
        .toSelect("select $$?$$ from b where c=:id")
        .argLong("id", 42L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void smartParsingIgnoresCharsInBracketedIdentifiers() throws Exception {
    // SQL Server bracketed identifier: '?' and ':' inside are not bind variables
    assertParsedSqlNoArgs(options, "select [?:missing] from t", "select [?:missing] from t");
    // Escaped ]] inside a bracketed identifier does not prematurely close it
    assertParsedSqlNoArgs(options, "select [a]]b?:c] from t", "select [a]]b?:c] from t");
    // Unterminated bracketed identifier is copied through verbatim
    assertParsedSqlNoArgs(options, "select [?:missing from t", "select [?:missing from t");
  }

  @Test
  public void smartParsingFindsParameterAfterBracketedIdentifier() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // A real parameter following a bracketed identifier column reference is still bound
    expect(c.prepareStatement("select [col?] from t where id=?")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(7)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, options)
        .toSelect("select [col?] from t where id=:id")
        .argLong("id", 7L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void legacyParsingCollapsesEscapedCharacters() throws Exception {
    // In legacy mode, '??' and '::' are treated as escapes and collapse to a single character
    assertParsedSqlNoArgs(optionsLegacyParsing, "select 'a??b::c' from dual", "select 'a?b:c' from dual");
  }

  @Test
  public void legacyParsingTreatsCastAsEscape() throws Exception {
    // In legacy mode, the PostgreSQL cast '::' is (incorrectly) collapsed to a single ':'
    assertParsedSqlNoArgs(optionsLegacyParsing, "select a::text from b", "select a:text from b");
  }

  @Test
  public void legacyParsingTreatsCharsInLiteralsAsParameters() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // In legacy mode, a '?' inside a string literal IS treated as a positional parameter
    expect(c.prepareStatement("select 'a?b' from dual")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(1)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, optionsLegacyParsing)
        .toSelect("select 'a?b' from dual").argLong(1L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void legacyParsingMixesPositionalAndNamedParameters() throws Exception {
    IMocksControl control = createStrictControl();

    Connection c = control.createMock(Connection.class);
    PreparedStatement ps = control.createMock(PreparedStatement.class);
    ResultSet rs = control.createMock(ResultSet.class);

    // In legacy mode, real positional (?) and named (:x) parameters still resolve correctly
    expect(c.prepareStatement("select a from b where c=? and d=? and e=1")).andReturn(ps);
    ps.setObject(eq(1), eq(Long.valueOf(1)));
    ps.setObject(eq(2), eq(Long.valueOf(2)));
    expect(ps.executeQuery()).andReturn(rs);
    expect(rs.next()).andReturn(false);
    rs.close();
    ps.close();

    control.replay();

    assertNull(new DatabaseImpl(c, optionsLegacyParsing)
        .toSelect("select a from b where c=? and d=:x and e=1")
        .argLong(1L).argLong("x", 2L).queryLongOrNull());

    control.verify();
  }

  @Test
  public void parsingMissingNamedParameterThrows() {
    DatabaseException ex = assertThrows(DatabaseException.class, () ->
        new DatabaseImpl(createNiceMock(Connection.class), options)
            .toSelect("select a from b where c=:missing").queryLongOrNull());
    assertThat(ex.getCause().getMessage(), containsString("requires parameter ':missing'"));
  }

  @Test
  public void parsingTooFewPositionalParametersThrows() {
    DatabaseException ex = assertThrows(DatabaseException.class, () ->
        new DatabaseImpl(createNiceMock(Connection.class), options)
            .toSelect("select a from b where c=? and d=?").argLong(1L).queryLongOrNull());
    assertThat(ex.getCause().getMessage(), containsString("Not enough positional parameters"));
  }

  @Test
  public void parsingTooManyPositionalParametersThrows() {
    DatabaseException ex = assertThrows(DatabaseException.class, () ->
        new DatabaseImpl(createNiceMock(Connection.class), options)
            .toSelect("select a from b where c=?").argLong(1L).argLong(2L).queryLongOrNull());
    assertThat(ex.getCause().getMessage(), containsString("Wrong number of positional parameters"));
  }

  @Test
  public void parsingUnusedNamedParameterThrows() {
    DatabaseException ex = assertThrows(DatabaseException.class, () ->
        new DatabaseImpl(createNiceMock(Connection.class), options)
            .toSelect("select a from b").argLong("unused", 2L).queryLongOrNull());
    assertThat(ex.getCause().getMessage(), containsString("do not exist in the query"));
  }

  @Test @Retry
  public void logFormatNoDebugSql() throws Exception {
    System.out.println(new DatabaseImpl(createNiceMock(DatabaseMock.class), options)
        .toSelect("select a from b where c=?")
        .argInteger(1)
        .queryLongOrNull());

    capturedLog.assertNoWarningsOrErrors();
    assertTrue(capturedLog.messages().get(0).endsWith("\tselect a from b where c=?"));
  }

  @Test
  public void logFormatDebugSqlInteger() throws Exception {
    System.out.println(new DatabaseImpl(createNiceMock(DatabaseMock.class), optionsFullLog)
        .toSelect("select a from b where c=?")
        .argInteger(1)
        .queryLongOrNull());

    capturedLog.assertNoWarningsOrErrors();
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect a from b where c=?${sep}select a from b where c=1");
  }

  @Test
  public void doNotLogSecrets() throws Exception {
    System.out.println(new DatabaseImpl(createNiceMock(DatabaseMock.class), optionsFullLog)
        .toSelect("select a from b where c=:secret_arg")
        .argInteger("secret_arg", 1)
        .queryLongOrNull());

    capturedLog.assertNoWarningsOrErrors();
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect a from b where c=?${sep}select a from b where c=<secret>");
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

    new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db, tx) -> {
      tx.setRollbackOnError(false);
      db.get();
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
      new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db, tx) -> {
        tx.setRollbackOnError(false);
        db.get();
        throw new Error("Oops");
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
      new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db, tx) -> {
        db.get();
        tx.setRollbackOnError(true);
        throw new DatabaseException("Oops");
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
      new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db, tx) -> {
        db.get();
        tx.setRollbackOnError(false);
        tx.setRollbackOnly(true);
        throw new DatabaseException("Oops");
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

    new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db, tx) -> {
      db.get();
      tx.setRollbackOnly(true);
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
      new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact(db -> {
        db.get();
        throw new Exception("Oops");
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

    new DatabaseProvider(() -> c, new OptionsDefault(Flavor.postgresql)).transact((db) -> {
      db.get();
    });

    control.verify();
  }

  @Test
  public void escapedParametersInLoggingShouldNotCauseWrongArgsMessage() {
    IMocksControl control = createStrictControl();

    DatabaseMock mock = control.createMock(DatabaseMock.class);
    expect(mock.query(anyString(), anyString())).andReturn(new RowStub()).anyTimes();

    control.replay();

    // With smart parsing (the default) a '?' inside a string literal is not a parameter
    new DatabaseImpl(mock, optionsFullLog)
        .toSelect("select 'test?value' as result, a from b where c=?")
        .argString("hi")
        .queryFirstOrNull(r -> r.getStringOrNull("result"));

    // ...and neither is a ':' inside a string literal
    new DatabaseImpl(mock, optionsFullLog)
        .toSelect("select 'test:value' as result, a from b where c=?")
        .argString("hi")
        .queryFirstOrNull(r -> r.getStringOrNull("result"));

    // Both kinds of character together inside a literal, alongside real bind variables
    new DatabaseImpl(mock, optionsFullLog)
        .toSelect("select 'test?value:end' as result, a from b where c=? and d=:param")
        .argString("hi")
        .argString("param", "test")
        .queryFirstOrNull(r -> r.getStringOrNull("result"));

    // Multiple literals in the same statement, none of them treated as parameters
    new DatabaseImpl(mock, optionsFullLog)
        .toSelect("select 'a?b:c' as result, a from b where c=? and d=:param union select 'd?e:f'")
        .argString("hi")
        .argString("param", "test")
        .queryFirstOrNull(r -> r.getStringOrNull("result"));

    // Verify the ParamSql was logged correctly for the cases above
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect 'test?value' as result, a from b where c=?${sep}select 'test?value' as result, a from b where c='hi'");
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect 'test:value' as result, a from b where c=?${sep}select 'test:value' as result, a from b where c='hi'");
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect 'test?value:end' as result, a from b where c=? and d=?${sep}select 'test?value:end' as result, a from b where c='hi' and d='test'");
    capturedLog.assertMessage(Level.DEBUG, "Query: ${timing}\tselect 'a?b:c' as result, a from b where c=? and d=? union select 'd?e:f'${sep}select 'a?b:c' as result, a from b where c='hi' and d='test' union select 'd?e:f'");

    control.verify();
  }

  private static class LogCaptureAppender extends AppenderSkeleton {
    private final List<LoggingEvent> events = new ArrayList<>();
    private int warnings = 0;
    private int errors = 0;

    protected synchronized void append(LoggingEvent event) {
      if (Level.TRACE.isGreaterOrEqual(event.getLevel())) {
        return;
      }
      if (event.getLevel().equals(Level.WARN)) {
        warnings++;
      } else if (event.getLevel().isGreaterOrEqual(Level.ERROR)) {
        errors++;
      }
      events.add(event);
    }

    public synchronized int nbrWarnings() {
      return warnings;
    }

    public synchronized int nbrErrors() {
      return errors;
    }

    public void close() {
      // Nothing to do
    }

    public synchronized String toString() {
      PatternLayout layout = new PatternLayout("%-5p %c %m\u00AE%n");
      StringBuilder builder = new StringBuilder();
      for (LoggingEvent event : events) {
        builder.append(layout.format(event));
      }
      return builder.toString();
    }

    public synchronized List<String> messages() {
      List<String> messages = new ArrayList<>();
      PatternLayout layout = new PatternLayout("%-5p %c %m");
      for (LoggingEvent event : events) {
        messages.add(layout.format(event));
      }
      return messages;
    }

    public synchronized void assertNoWarningsOrErrors() {
      assertEquals("Warnings or errors in the log:\n" + toString(), 0, nbrWarnings() + nbrErrors());
    }

    public synchronized void assertWarnings(int nbrWarnings) {
      assertEquals("Wrong number of warnings in the log:\n" + toString(), nbrWarnings, nbrWarnings());
    }

    public synchronized void assertErrors(int nbrErrors) {
      assertEquals("Wrong number of errors or above in the log:\n" + toString(), nbrErrors, nbrErrors());
    }

    public synchronized void assertEntries(int nbrEntries) {
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
    public synchronized void assertMessage(Level level, String messagePattern) {
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
