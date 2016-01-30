package com.github.susom.database.test;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.Properties;

import org.junit.Test;

import com.github.susom.database.Config;

import static org.junit.Assert.*;

/**
 * Unit tests for the configuration classes.
 *
 * @author garricko
 */
public class ConfigTest {
  @Test
  public void testSystemProperties() throws Exception {
    System.setProperty("foo", "bar");

    Config config = Config.from().systemProperties().get();

    assertEquals("bar", config.getString("foo"));
    assertNull(config.getString("unknown"));
    assertEquals("default", config.getString("unknown", "default"));
  }

  @Test
  public void testProperties() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("foo", "bar");

    Config config = Config.from().properties(properties).get();

    assertEquals("bar", config.getString("foo"));
    assertNull(config.getString("unknown"));
    assertEquals("default", config.getString("unknown", "default"));
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
    Config config = Config.from().propertyFile(filename1, null, filename2).get();

    assertEquals(new Integer(1), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
    assertNull(config.getInteger("unknown"));
    assertEquals(5, config.getInteger("unknown", 5));

    // Now flip the order and verify precedence works
    config = Config.from().propertyFile(filename2, null, filename1).get();
    assertEquals(new Integer(2), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));

    // Same as above tests, but using File version rather than filename String
    config = Config.from().propertyFile(new File(filename1), new File("does not exist"), new File(filename2)).get();

    assertEquals(new Integer(1), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
    assertNull(config.getInteger("unknown"));
    assertEquals(5, config.getInteger("unknown", 5));

    // Now flip the order and verify precedence works
    config = Config.from().propertyFile(new File(filename2), null, new File(filename1)).get();
    assertEquals(new Integer(2), config.getInteger("foo"));
    assertEquals(new Integer(-2), config.getInteger("foo2"));
  }

  @Test
  public void testNested() throws Exception {
    Config config = Config.from()
        .config(Config.from().custom(key -> key.equals("foo") ? "a" : null))
        .config(Config.from().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("a", config.getString("foo"));

    // Re-mapping prefix in nested config
    config = Config.from()
        .config(Config.from().custom(key -> key.equals("a.foo") ? "a" : null).removePrefix("a."))
        .config(Config.from().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("a", config.getString("foo"));

    // Excluding nested config, should skip to next
    config = Config.from()
        .config(Config.from().custom(key -> key.equals("a.foo") ? "a" : null).removePrefix("a.").excludeRegex("fo{2}"))
        .config(Config.from().custom(key -> key.equals("foo") ? "b" : null)).get();

    assertEquals("b", config.getString("foo"));
    assertNull(config.getString("foooo"));

    config = Config.from()
        .config(Config.from().custom(key -> key.equals("a.foo") ? "a" : null).excludePrefix("a.", "other."))
        .config(Config.from().custom(key -> key.equals("foo") ? "b" : null).addPrefix("a.")).get();

    assertEquals("b", config.getString("a.foo"));
    assertNull(config.getString("foo"));

    config = Config.from()
        .config(Config.from().custom(key -> key.equals("a.foo") ? "a" : null).includePrefix("other."))
        .config(Config.from().custom(key -> key.equals("foo") ? "b" : null).addPrefix("a.").includeRegex("a.*f.*")).get();

    assertEquals("b", config.getString("a.foo"));
    assertNull(config.getString("foo"));
    assertNull(config.getString("other.foo"));
  }

  @Test
  public void testStripPrefixConflict() throws Exception {
    Config config = Config.from().value("a.foo", "a").value("foo", "bar").removePrefix("a.").get();

    assertEquals("bar", config.getString("foo"));
  }

  @Test
  public void testException() throws Exception {
    Config config = Config.from().custom(key -> {
      throw new SecurityException("Pretending security policy is in place");
    }).get();

    // We do this call twice, but you should see only one warning in the log
    assertEquals("default", config.getString("foo", "default"));
    assertEquals("default", config.getString("foo", "default"));
  }

  @Test
  public void testTidyValues() throws Exception {
    Config config = Config.from().value("foo", " a ").get();

    // Strip whitespace
    assertEquals("a", config.getString("foo"));

    config = Config.from().value("foo", " ").value("foo", "").value("foo", null).value("foo", "a").get();

    // Skip over the garbage ones
    assertEquals("a", config.getString("foo"));
  }

  @Test
  public void testBoolean() throws Exception {
    // Case insensitive, allow either true/false or yes/no
    Config config = Config.from().value("foo", "tRuE").get();

    assertTrue(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = Config.from().value("foo", "yEs").get();

    assertTrue(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = Config.from().value("foo", "fAlSe").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertFalse(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = Config.from().value("foo", "nO").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertFalse(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));

    config = Config.from().value("foo", "bad value").get();

    assertFalse(config.getBooleanOrFalse("foo"));
    assertTrue(config.getBooleanOrTrue("foo"));
    assertFalse(config.getBooleanOrFalse("unknown"));
    assertTrue(config.getBooleanOrTrue("unknown"));
  }

  @Test
  public void testInteger() throws Exception {
    Config config = Config.from().value("good", "123").value("bad", "hi").get();

    assertEquals(new Integer(123), config.getInteger("good"));
    assertNull(config.getInteger("bad"));
    assertNull(config.getInteger("missing"));
    assertEquals(123, config.getInteger("good", 5));
    assertEquals(5, config.getInteger("bad", 5));
    assertEquals(5, config.getInteger("missing", 5));
  }

  @Test
  public void testLong() throws Exception {
    Config config = Config.from().value("good", "123").value("bad", "hi").get();

    assertEquals(new Long(123), config.getLong("good"));
    assertNull(config.getLong("bad"));
    assertNull(config.getLong("missing"));
    assertEquals(123, config.getLong("good", 5));
    assertEquals(5, config.getLong("bad", 5));
    assertEquals(5, config.getLong("missing", 5));
  }

  @Test
  public void testFloat() throws Exception {
    Config config = Config.from().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new Float(123.45f), config.getFloat("good"));
    assertNull(config.getFloat("bad"));
    assertNull(config.getFloat("missing"));
    assertEquals(123.45, config.getFloat("good", 5.45f), 0.001);
    assertEquals(5.45, config.getFloat("bad", 5.45f), 0.001);
    assertEquals(5.45, config.getFloat("missing", 5.45f), 0.001);
  }

  @Test
  public void testDouble() throws Exception {
    Config config = Config.from().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new Double(123.45), config.getDouble("good"));
    assertNull(config.getDouble("bad"));
    assertNull(config.getDouble("missing"));
    assertEquals(123.45, config.getDouble("good", 5.45), 0.001);
    assertEquals(5.45, config.getDouble("bad", 5.45), 0.001);
    assertEquals(5.45, config.getDouble("missing", 5.45), 0.001);
  }

  @Test
  public void testBigDecimal() throws Exception {
    Config config = Config.from().value("good", "123.45").value("bad", "hi").get();

    assertEquals(new BigDecimal("123.45"), config.getBigDecimal("good"));
    assertNull(config.getBigDecimal("bad"));
    assertNull(config.getBigDecimal("missing"));
    assertEquals(new BigDecimal("123.45"), config.getBigDecimal("good", new BigDecimal("5.45")));
    assertEquals(new BigDecimal("5.45"), config.getBigDecimal("bad", new BigDecimal("5.45")));
    assertEquals(new BigDecimal("5.45"), config.getBigDecimal("missing", new BigDecimal("5.45")));
  }
}
