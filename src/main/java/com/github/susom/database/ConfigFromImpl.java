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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  public ConfigFrom custom(Function<String, String> keyValueLookup) {
    return custom(keyValueLookup::apply, "custom()");
  }

  private ConfigFrom custom(Function<String, String> keyValueLookup, String source) {
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
  public ConfigFrom config(Supplier<Config> config) {
    return config(config.get());
  }

  @Override
  public ConfigFrom systemProperties() {
    return custom(System::getProperty, "systemProperties()");
  }

  @Override
  public ConfigFrom env() {
    return custom(System::getenv, "env()");
  }

  @Override
  public ConfigFrom properties(Properties properties) {
    return custom(properties::getProperty, "properties()");
  }

  @Override
  public ConfigFrom defaultPropertyFiles() {
    return defaultPropertyFiles("properties", "conf/app.properties", "local.properties", "sample.properties");
  }

  @Override
  public ConfigFrom defaultPropertyFiles(String systemPropertyKey, String... filenames) {
    return defaultPropertyFiles(systemPropertyKey, Charset.defaultCharset().newDecoder(), filenames);
  }

  @Override
  public ConfigFrom defaultPropertyFiles(String systemPropertyKey, CharsetDecoder decoder, String... filenames) {
    String properties = System.getProperty(systemPropertyKey, String.join(File.pathSeparator, filenames));
    return propertyFile(Charset.defaultCharset().newDecoder(), properties.split(File.pathSeparator));
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
  public ConfigFrom rename(String configKey, String newKey) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      if (key.equals(configKey)) {
        return null;
      }
      if (key.equals(newKey)) {
        return lookup(configKey);
      }
      return lookup(key);
    }, indentedSources("rename(" + configKey + " -> " + newKey + ")")));
  }

  @Override
  public ConfigFrom includeKeys(String... keys) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      for (String k : keys) {
        if (key.equals(k)) {
          return lookup(key);
        }
      }
      return null;
    }, indentedSources("includeKeys" + Arrays.asList(keys))));
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
  public ConfigFrom excludeKeys(String... keys) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      for (String k : keys) {
        if (key.equals(k)) {
          return null;
        }
      }
      return lookup(key);
    }, indentedSources("excludeKeys" + Arrays.asList(keys))));
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
  public ConfigFrom substitutions(Config config) {
    return new ConfigFromImpl(new ConfigImpl(key -> {
      String value = lookup(key);
      if (value != null) {
        // matches ${ENV_VAR_NAME} or $ENV_VAR_NAME
        Pattern p = Pattern.compile("(?<!\\$)\\$(?!\\$)\\{(\\w+)\\}|(?<!\\$)\\$(?!\\$)(\\w+)");
        Matcher m = p.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
          String envVarName = null == m.group(1) ? m.group(2) : m.group(1);
          String envVarValue = config.getString(envVarName);
          m.appendReplacement(sb, null == envVarValue ? "" : envVarValue);
        }
        m.appendTail(sb);
        // Allow escaping literal $ with $$
        return sb.toString().replaceAll("(\\${2})", "\\$");
      } else {
        return null;
      }
    }, indentedSources("substitutions(" + config.sources() + ")")));
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
