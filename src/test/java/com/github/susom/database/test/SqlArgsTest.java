package com.github.susom.database.test;

import org.junit.Test;

import com.github.susom.database.SqlArgs;

import static org.junit.Assert.*;

/**
 * Unit tests for the SqlArgs class.
 *
 * @author garricko
 */
public class SqlArgsTest {
  @Test
  public void testTidyColumnNames() throws Exception {
    assertArrayEquals(new String[] { "column_1", "column_2", "a", "a_2", "a_3", "a1" },
        SqlArgs.tidyColumnNames(new String[] { null, "", " a ", "a  ", "#!@#$_a","#!@#$_1" }));

    check("TheBest", "the_best");
  }

  private void check(String input, String output) {
    assertArrayEquals(new String[] { output }, SqlArgs.tidyColumnNames(new String[] { input }));
  }
}
