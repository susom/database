package com.github.susom.database.test;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import com.github.susom.database.Config;
import com.github.susom.database.ConfigFrom;

import static org.junit.Assert.*;

/**
 * Unit tests for the configuration classes.
 *
 * @author garricko
 */
public class ConfigTest {

  @Rule
  public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testSystemProperties() throws Exception {
    System.setProperty("foo", "bar");

    Config config = ConfigFrom.firstOf().systemProperties().get();

    assertEquals("bar", config.getString("foo"));
    assertNull(config.getString("unknown"));
    assertEquals("default", config.getString("unknown", "default"));
  }

  @Test
  public void testProperties() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("foo", "bar");

    Config config = ConfigFrom.firstOf().properties(properties).get();

    assertEquals("bar", config.getString("foo"));
    assertNull(config.getString("unknown"));
    assertEquals("default", config.getString("unknown", "default"));
  }

  @Test
  public void testEnvironmentSubstitution() throws Exception {
    environmentVariables.set("FOO", "bar");
    environmentVariables.set("PREFIX_FOO", "baz");

    Config env = ConfigFrom.firstOf().env().get();

    // Test curly brace substitution and escaped $
    Config config = ConfigFrom.firstOf().value("test", "${FOO}-$$").substitutions(env).get();
    assertEquals("bar-$", config.getString("test"));

    // Test without curly braces
    config = ConfigFrom.firstOf().value("test", "$FOO-$$-$FOO").substitutions(env).get();
    assertEquals("bar-$-bar", config.getString("test"));

    // Test non-existing env var
    config = ConfigFrom.firstOf().value("test", "abc${XXQQXX}def").substitutions(env).get();
    assertEquals("abcdef", config.getString("test"));

    // Test filtering environment variables with a regex
    env = ConfigFrom.firstOf().env().includeRegex("PREFIX_.*").get();
    config = ConfigFrom.firstOf().value("test", "${PREFIX_FOO}-${FOO}").substitutions(env).get();
    assertEquals("baz-", config.getString("test"));
  }

  @Test
  public void testPropertyFiles() throws Exception {
    Properties properties = new Properties();

    properties.setProperty("foo", "1");
    String filename1 = "target/ConfigTest-properties-1.properties";
    properties.store(new FileWriter(filename1), null);

    properties.setProperty("foo", "2");
    properties.setProperty("foo2", "-2");
    String filename2 = "target/ConfigTest-properties-2.properties";
    properties.store(new FileWriter(filename2), null);

    // Throw a null in here just to make sure it doesn't blow up
    Config config = ConfigFrom.firstOf().propertyFile(filename1, null, filename2).get();

    assertEquals(new Integer(1), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
    assertNull(config.getInteger("unknown"));
    assertEquals(5, config.getInteger("unknown", 5));

    // Now flip the order and verify precedence works
    config = ConfigFrom.firstOf().propertyFile(filename2, null, filename1).get();
    assertEquals(new Integer(2), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));

    // Same as above tests, but using File version rather than filename String
    config = ConfigFrom.firstOf().propertyFile(new File(filename1), new File("does not exist"), new File(filename2)).get();

    assertEquals(new Integer(1), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
    assertNull(config.getInteger("unknown"));
    assertEquals(5, config.getInteger("unknown", 5));

    // Now flip the order and verify precedence works
    config = ConfigFrom.firstOf().propertyFile(new File(filename2), null, new File(filename1)).get();
    assertEquals(new Integer(2), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
  }

  @Test
  public void testNested() throws Exception {
    Config config = ConfigFrom.firstOf()
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "a" : null))
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("a", config.getString("foo"));

    // Re-mapping prefix in nested config
    config = ConfigFrom.firstOf()
        .config(ConfigFrom.firstOf().custom(key -> key.equals("a.foo") ? "a" : null).removePrefix("a."))
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("a", config.getString("foo"));

    // Excluding nested config, should skip to next
    config = ConfigFrom.firstOf()
        .config(ConfigFrom.firstOf().custom(key -> key.equals("a.foo") ? "a" : null).removePrefix("a.").excludeRegex("fo{2}"))
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("b", config.getString("foo"));
    assertNull(config.getString("foooo"));

    config = ConfigFrom.firstOf()
        .config(ConfigFrom.firstOf().custom(key -> key.equals("a.foo") ? "a" : null).excludePrefix("a.", "other."))
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "b" : null).addPrefix("a.")).get();

    assertEquals("b", config.getString("a.foo"));
    assertNull(config.getString("foo"));

    config = ConfigFrom.firstOf()
        .config(ConfigFrom.firstOf().custom(key -> key.equals("a.foo") ? "a" : null).includePrefix("other."))
        .config(ConfigFrom.firstOf().custom(key -> key.equals("foo") ? "b" : null).addPrefix("a.").includeRegex("a.*f.*")).get();

    assertEquals("b", config.getString("a.foo"));
    assertNull(config.getString("foo"));
    assertNull(config.getString("other.foo"));
  }

  @Test
  public void testStripPrefixConflict() throws Exception {
    Config config = ConfigFrom.firstOf().value("a.foo", "a").value("foo", "bar").removePrefix("a.").get();

    assertEquals("bar", config.getString("foo"));
  }

  @Test
  public void testException() throws Exception {
    Config config = ConfigFrom.firstOf().custom(key -> {
      throw new SecurityException("Pretending security policy is in place");
    }).get();

    // We do this call twice, but you should see only one warning in the log
    assertEquals("default", config.getString("foo", "default"));
    assertEquals("default", config.getString("foo", "default"));
  }

  @Test
  public void testTidyValues() throws Exception {
    Config config = ConfigFrom.firstOf().value("foo", " a ").get();

    // Strip whitespace
    assertEquals("a", config.getString("foo"));

    config = ConfigFrom.firstOf().value("foo", " ").value("foo", "").value("foo", null).value("foo", "a").get();

    // Skip over the garbage ones
    assertEquals("a", config.getString("foo"));
  }

  @Test
  public void testBoolean() throws Exception {
    // Case insensitive, allow either true/false or yes/no
    Config config = ConfigFrom.firstOf().value("foo", "tRuE").get();

    assertTrue(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = ConfigFrom.firstOf().value("foo", "yEs").get();

    assertTrue(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = ConfigFrom.firstOf().value("foo", "fAlSe").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertFalse(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = ConfigFrom.firstOf().value("foo", "nO").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertFalse(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = ConfigFrom.firstOf().value("foo", "bad value").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));
  }

  @Test
  public void testInteger() throws Exception {
    Config config = ConfigFrom.firstOf().value("good", "123").value("bad", "hi").get();

    assertEquals(new Integer(123), config.getInteger("good"));
    assertNull(config.getInteger("bad"));
    assertNull(config.getInteger("missing"));
    assertEquals(123, config.getInteger("good", 5));
    assertEquals(5, config.getInteger("bad", 5));
    assertEquals(5, config.getInteger("missing", 5));
  }

  @Test
  public void testLong() throws Exception {
    Config config = ConfigFrom.firstOf().value("good", "123").value("bad", "hi").get();

    assertEquals(new Long(123), config.getLong("good"));
    assertNull(config.getLong("bad"));
    assertNull(config.getLong("missing"));
    assertEquals(123, config.getLong("good", 5));
    assertEquals(5, config.getLong("bad", 5));
    assertEquals(5, config.getLong("missing", 5));
  }

  @Test
  public void testFloat() throws Exception {
    Config config = ConfigFrom.firstOf().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new Float(123.45f), config.getFloat("good"));
    assertNull(config.getFloat("bad"));
    assertNull(config.getFloat("missing"));
    assertEquals(123.45, config.getFloat("good", 5.45f), 0.001);
    assertEquals(5.45, config.getFloat("bad", 5.45f), 0.001);
    assertEquals(5.45, config.getFloat("missing", 5.45f), 0.001);
  }

  @Test
  public void testDouble() throws Exception {
    Config config = ConfigFrom.firstOf().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new Double(123.45), config.getDouble("good"));
    assertNull(config.getDouble("bad"));
    assertNull(config.getDouble("missing"));
    assertEquals(123.45, config.getDouble("good", 5.45), 0.001);
    assertEquals(5.45, config.getDouble("bad", 5.45), 0.001);
    assertEquals(5.45, config.getDouble("missing", 5.45), 0.001);
  }

  @Test
  public void testBigDecimal() throws Exception {
    Config config = ConfigFrom.firstOf().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new BigDecimal("123.45"), config.getBigDecimal("good"));
    assertNull(config.getBigDecimal("bad"));
    assertNull(config.getBigDecimal("missing"));
    assertEquals(new BigDecimal("123.45"), config.getBigDecimal("good", new BigDecimal("5.45")));
    assertEquals(new BigDecimal("5.45"), config.getBigDecimal("bad", new BigDecimal("5.45")));
    assertEquals(new BigDecimal("5.45"), config.getBigDecimal("missing", new BigDecimal("5.45")));
  }
}
