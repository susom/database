package com.github.susom.database.test;

import com.github.susom.database.*;
import org.junit.Test;

import java.time.LocalDate;
import java.time.Month;

import static org.junit.Assert.*;

/**
 * Unit tests for the SqlArgs class.
 *
 * @author garricko
 */
public class SqlArgsTest {

  @Test
  public void testLocalDateArgs() {
    SqlArgs args = new SqlArgs();
    args.argLocalDate(LocalDate.of(2019, Month.JANUARY, 1));
    args.argLocalDate(LocalDate.of(2019, Month.DECEMBER, 31));

    assertEquals(2, args.argCount());
    assertEquals("SqlArgs[{name=null, type=LocalDate, arg=2019-01-01}, {name=null, type=LocalDate, arg=2019-12-31}]",
      args.toString());
  }

  public void testTidyColumnNames() throws Exception {
    assertArrayEquals(new String[] { "column_1", "column_2", "a", "a_2", "a_3", "a1" },
        SqlArgs.tidyColumnNames(new String[] { null, "", " a ", "a  ", "#!@#$_a","#!@#$_1" }));

    check("TheBest", "the_best");
  }

  private void check(String input, String output) {
    assertArrayEquals(new String[] { output }, SqlArgs.tidyColumnNames(new String[] { input }));
  }
}
