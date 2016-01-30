package com.github.susom.database;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Access configuration properties from a variety of standard sources,
 * and provide some basic filtering and mapping of property keys.
 *
 * @author garricko
 */
public class ConfigFromImpl implements ConfigFrom {
  private static final Logger log = LoggerFactory.getLogger(ConfigFromImpl.class);
  private List<Config> searchPath = new ArrayList<>();

  public ConfigFromImpl() {
    super();
  }

  public ConfigFromImpl(Config first) {
    searchPath.add(first);
  }

  @Override
  public ConfigFrom custom(ConfigStrings keyValueLookup) {
    searchPath.add(new ConfigImpl(keyValueLookup));
    return this;
  }

  @Override
  public ConfigFrom value(String key, String value) {
    return custom(k -> k.equals(key) ? value : null);
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
    return custom(System::getProperty);
  }

  @Override
  public ConfigFrom properties(Properties properties) {
    return custom(properties::getProperty);
  }

  @Override
  public ConfigFrom propertyFile(String... filenames) {
    for (String filename : filenames) {
      if (filename != null) {
        propertyFile(new File(filename));
      }
    }
    return this;
  }

  @Override
  public ConfigFrom propertyFile(File... files) {
    for (File file : files) {
      if (file != null) {
        try {
          Properties properties = new Properties();
          properties.load(new FileReader(file));
          searchPath.add(new ConfigImpl(properties::getProperty));
          if (log.isTraceEnabled()) {
            log.trace("Using properties from file: " + file.getAbsolutePath());
          }
        } catch (Exception e) {
          // Don't care, fallback to system properties
          if (log.isTraceEnabled()) {
            log.trace("Unable to load properties from file: " + file.getAbsolutePath(), e);
          }
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
    }));
  }

  @Override
  public ConfigFrom includeRegex(String regex) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.matches(regex)) {
        return lookup(key);
      }
      return null;
    }));
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
    }));
  }

  @Override
  public ConfigFrom excludeRegex(String regex) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.matches(regex)) {
        return null;
      }
      return lookup(key);
    }));
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
    }));
  }

  @Override
  public ConfigFrom addPrefix(String prefix) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.startsWith(prefix)) {
        return lookup(key.substring(prefix.length()));
      } else {
        return null;
      }
    }));
  }

  @Override
  public Config get() {
    return new ConfigImpl(this::lookup);
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
