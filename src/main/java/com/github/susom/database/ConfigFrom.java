package com.github.susom.database;

import java.io.File;
import java.nio.charset.CharsetDecoder;
import java.util.Properties;

/**
 * Pull configuration properties from various sources and filter/manipulate them.
 *
 * @author garricko
 */
public interface ConfigFrom {
  ConfigFrom custom(ConfigStrings keyValueLookup);

  ConfigFrom value(String key, String value);

  ConfigFrom systemProperties();

  ConfigFrom properties(Properties properties);

  ConfigFrom config(Config config);

  ConfigFrom config(ConfigFrom config);

  ConfigFrom propertyFile(String... filenames);

  ConfigFrom propertyFile(CharsetDecoder decoder, String... filenames);

  ConfigFrom propertyFile(File... files);

  ConfigFrom propertyFile(CharsetDecoder decoder, File... files);

  ConfigFrom includePrefix(String... prefixes);

  ConfigFrom includeRegex(String regex);

  ConfigFrom excludePrefix(String... prefixes);

  ConfigFrom excludeRegex(String regex);

  ConfigFrom removePrefix(String... prefixes);

  ConfigFrom addPrefix(String prefix);

  Config get();
}
