package com.github.susom.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Access configuration properties from a variety of standard sources,
 * and provide some basic filtering and mapping of property keys.
 *
 * @author garricko
 */
public class ConfigFromImpl implements ConfigFrom {
  private List<Config> searchPath = new ArrayList<>();

  public ConfigFromImpl() {
    super();
  }

  public ConfigFromImpl(Config first) {
    searchPath.add(first);
  }

  @Override
  public ConfigFrom custom(ConfigStrings keyValueLookup) {
    return custom(keyValueLookup, "custom()");
  }

  private ConfigFrom custom(ConfigStrings keyValueLookup, String source) {
    searchPath.add(new ConfigImpl(keyValueLookup, source));
    return this;
  }

  @Override
  public ConfigFrom value(String key, String value) {
    return custom(k -> k.equals(key) ? value : null, "value(" + key + ")");
  }

  @Override
  public ConfigFrom config(Config config) {
    searchPath.add(config);
    return this;
  }

  @Override
  public ConfigFrom config(ConfigFrom config) {
    return config(config.get());
  }

  @Override
  public ConfigFrom systemProperties() {
    return custom(System::getProperty, "systemProperties()");
  }

  @Override
  public ConfigFrom properties(Properties properties) {
    return custom(properties::getProperty, "properties()");
  }

  @Override
  public ConfigFrom propertyFile(String... filenames) {
    return propertyFile(Charset.defaultCharset().newDecoder(), filenames);
  }

  @Override
  public ConfigFrom propertyFile(CharsetDecoder decoder, String... filenames) {
    for (String filename : filenames) {
      if (filename != null) {
        propertyFile(decoder, new File(filename));
      }
    }
    return this;
  }

  @Override
  public ConfigFrom propertyFile(File... files) {
    return propertyFile(Charset.defaultCharset().newDecoder(), files);
  }

  @Override
  public ConfigFrom propertyFile(CharsetDecoder decoder, File... files) {
    for (File file : files) {
      if (file != null) {
        try {
          Properties properties = new Properties();
          properties.load(new InputStreamReader(new FileInputStream(file), decoder));
          searchPath.add(new ConfigImpl(properties::getProperty, "propertyFile(" + file.getAbsolutePath() + ")"));
        } catch (Exception e) {
          // Put a "fake" provider in so we can see it failed
          String fileName = file.getName();
          try {
            fileName = file.getAbsolutePath();
          } catch (Exception ignored) {
            // Fall back to relative name
          }
          custom(k -> null, "Ignored: propertyFile(" + fileName + ") " + e.getClass().getSimpleName());
        }
      }
    }
    return this;
  }

  @Override
  public ConfigFrom includePrefix(String... prefixes) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          return lookup(key);
        }
      }
      return null;
    }, indentedSources("includePrefix" + Arrays.asList(prefixes))));
  }

  @Override
  public ConfigFrom includeRegex(String regex) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.matches(regex)) {
        return lookup(key);
      }
      return null;
    }, indentedSources("includeRegex(" + regex + ")")));
  }

  @Override
  public ConfigFrom excludePrefix(String... prefixes) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          return null;
        }
      }
      return lookup(key);
    }, indentedSources("excludePrefix" + Arrays.asList(prefixes))));
  }

  @Override
  public ConfigFrom excludeRegex(String regex) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.matches(regex)) {
        return null;
      }
      return lookup(key);
    }, indentedSources("excludeRegex(" + regex + ")")));
  }

  @Override
  public ConfigFrom removePrefix(String... prefixes) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      // Give precedence to ones that already lacked the prefix,
      // do an include*() first if you don't want that
      String value = lookup(key);
      if (value != null) {
        return value;
      }

      for (String prefix : prefixes) {
        value = lookup(prefix + key);
        if (value != null) {
          return value;
        }
      }
      return null;
    }, indentedSources("removePrefix" + Arrays.asList(prefixes))));
  }

  @Override
  public ConfigFrom addPrefix(String prefix) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.startsWith(prefix)) {
        return lookup(key.substring(prefix.length()));
      } else {
        return null;
      }
    }, indentedSources("addPrefix(" + prefix + ")")));
  }

  @Override
  public Config get() {
    return new ConfigImpl(this::lookup, indentedSources("Config"));
  }

  private String indentedSources(String label) {
    StringBuilder buf = new StringBuilder(label);
    for (Config config : searchPath) {
      buf.append(config.sources().replaceAll("(?s)^|\\n", "\n  "));
    }
    return buf.toString();
  }

  private String lookup(String key) {
    for (Config config : searchPath) {
      String value = config.getString(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }
}
