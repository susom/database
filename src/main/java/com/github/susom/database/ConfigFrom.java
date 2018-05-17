package com.github.susom.database;

import java.io.File;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

/**
 * Pull configuration properties from various sources and filter/manipulate them.
 *
 * @author garricko
 */
public interface ConfigFrom extends Supplier<Config> {
  /**
   * Convenience method for fluent syntax.
   *
   * @return a builder for specifying from where configuration should be loaded
   */
  @Nonnull
  static ConfigFrom firstOf() {
    return new ConfigFromImpl();
  }

  @Nonnull
  static Config other(Function<String, String> other) {
    if (other instanceof Config) {
      return (Config) other;
    }
    return new ConfigFromImpl().custom(other::apply).get();
  }

  ConfigFrom custom(Function<String, String> keyValueLookup);

  ConfigFrom value(String key, String value);

  ConfigFrom systemProperties();

  ConfigFrom env();

  ConfigFrom properties(Properties properties);

  ConfigFrom config(Config config);

  ConfigFrom config(Supplier<Config> config);

  /**
   * Adds a set of properties files to read from, which can be overridden by a system property "properties".
   * Equivalent to:
   * <pre>
   *   defaultPropertyFiles("properties", "conf/app.properties", "local.properties", "sample.properties")
   * </pre>
   */
  ConfigFrom defaultPropertyFiles();

  /**
   * Adds a set of properties files to read from, which can be overridden by a specified system property.
   * Equivalent to:
   * <pre>
   *   defaultPropertyFiles(systemPropertyKey, Charset.defaultCharset().newDecoder(), filenames)
   * </pre>
   */
  ConfigFrom defaultPropertyFiles(String systemPropertyKey, String... filenames);

  /**
   * Adds a set of properties files to read from, which can be overridden by a specified system property.
   * Equivalent to:
   * <pre>
   *   propertyFile(Charset.defaultCharset().newDecoder(),
   *       System.getProperty(systemPropertyKey, String.join(File.pathSeparator, filenames))
   *       .split(File.pathSeparator));
   * </pre>
   */
  ConfigFrom defaultPropertyFiles(String systemPropertyKey, CharsetDecoder decoder, String... filenames);

  ConfigFrom propertyFile(String... filenames);

  ConfigFrom propertyFile(CharsetDecoder decoder, String... filenames);

  ConfigFrom propertyFile(File... files);

  ConfigFrom propertyFile(CharsetDecoder decoder, File... files);

  ConfigFrom rename(String key, String newKey);

  ConfigFrom includeKeys(String... keys);

  ConfigFrom includePrefix(String... prefixes);

  ConfigFrom includeRegex(String regex);

  ConfigFrom excludeKeys(String... keys);

  ConfigFrom excludePrefix(String... prefixes);

  ConfigFrom excludeRegex(String regex);

  ConfigFrom removePrefix(String... prefixes);

  ConfigFrom addPrefix(String prefix);

  Config get();
}
