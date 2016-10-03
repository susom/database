package com.github.susom.database;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles all of the type conversions, default values, etc.
 * to get from simple string key/value pairs to richer configuration.
 *
 * @author garricko
 */
public class ConfigImpl implements Config {
  private static final Logger log = LoggerFactory.getLogger(ConfigFromImpl.class);
  private final Function<String, String> provider;
  private final String sources;
  private Set<String> failedKeys = new HashSet<>();

  public ConfigImpl(Function<String, String> provider, String sources) {
    this.provider = provider;
    this.sources = sources;
  }

  @Override
  public String getString(@Nonnull String key) {
    return cleanString(key);
  }

  @Nonnull
  @Override
  public String getStringOrThrow(@Nonnull String key) {
    return nonnull(key, getString(key));
  }

  @Nonnull
  @Override
  public String getString(String key, @Nonnull String defaultValue) {
    String stringValue = cleanString(key);
    if (stringValue != null) {
      return stringValue;
    }
    // Make sure the default value is tidied the same way a value would be
    defaultValue = defaultValue.trim();
    if (defaultValue.length() == 0) {
      throw new IllegalArgumentException("Your default value is empty or just whitespace");
    }
    return defaultValue;
  }

  @Override
  public Integer getInteger(@Nonnull String key) {
    String stringValue = cleanString(key);
    try {
      return stringValue == null ? null : Integer.parseInt(stringValue);
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
      return null;
    }
  }

  @Override
  public int getInteger(@Nonnull String key, int defaultValue) {
    Integer value = getInteger(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public int getIntegerOrThrow(@Nonnull String key) {
    return nonnull(key, getInteger(key));
  }

  @Nullable
  @Override
  public Long getLong(@Nonnull String key) {
    String stringValue = cleanString(key);
    try {
      return stringValue == null ? null : Long.parseLong(stringValue);
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
      return null;
    }
  }

  @Override
  public long getLong(@Nonnull String key, long defaultValue) {
    Long value = getLong(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public long getLongOrThrow(@Nonnull String key) {
    return nonnull(key, getLong(key));
  }

  @Nullable
  @Override
  public Float getFloat(@Nonnull String key) {
    String stringValue = cleanString(key);
    try {
      return stringValue == null ? null : Float.parseFloat(stringValue);
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
      return null;
    }
  }

  @Override
  public float getFloat(@Nonnull String key, float defaultValue) {
    Float value = getFloat(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public float getFloatOrThrow(@Nonnull String key) {
    return nonnull(key, getFloat(key));
  }

  @Nullable
  @Override
  public Double getDouble(@Nonnull String key) {
    String stringValue = cleanString(key);
    try {
      return stringValue == null ? null : Double.parseDouble(stringValue);
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
      return null;
    }
  }

  @Override
  public double getDouble(@Nonnull String key, double defaultValue) {
    Double value = getDouble(key);
    return value == null ? defaultValue : value;
  }

  @Override
  public double getDoubleOrThrow(@Nonnull String key) {
    return nonnull(key, getDouble(key));
  }

  @Nullable
  @Override
  public BigDecimal getBigDecimal(@Nonnull String key) {
    String stringValue = cleanString(key);
    try {
      return stringValue == null ? null : new BigDecimal(stringValue);
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
      return null;
    }
  }

  @Nonnull
  @Override
  public BigDecimal getBigDecimal(String key, @Nonnull BigDecimal defaultValue) {
    BigDecimal value = getBigDecimal(key);
    return value == null ? defaultValue : value;
  }

  @Nonnull
  @Override
  public BigDecimal getBigDecimalOrThrow(String key) {
    return nonnull(key, getBigDecimal(key));
  }

  @Override
  public boolean getBooleanOrFalse(@Nonnull String key) {
    return parseBoolean(cleanString(key), false);
  }

  @Override
  public boolean getBooleanOrTrue(@Nonnull String key) {
    return parseBoolean(cleanString(key), true);
  }

  @Override
  public boolean getBooleanOrThrow(@Nonnull String key) {
    String value = nonnull(key, cleanString(key));
    value = value.toLowerCase();
    if (value.equals("yes") || value.equals("true")) {
      return true;
    }
    if (value.equals("no") || value.equals("false")) {
      return false;
    }
    throw new ConfigMissingException("Unrecognized boolean value for config key: " + key);
  }

  @Override
  public String sources() {
    return sources;
  }

  private <T> T nonnull(String key, T value) {
    if (value == null) {
      throw new ConfigMissingException("No value for config key: " + key);
    }
    return value;
  }

  private boolean parseBoolean(String value, boolean defaultValue) {
    if (value != null) {
      value = value.toLowerCase();
      if (value.equals("yes") || value.equals("true")) {
        return true;
      }
      if (value.equals("no") || value.equals("false")) {
        return false;
      }
    }
    return defaultValue;
  }

  private String cleanString(String key) {
    String value = null;
    try {
      value = provider.apply(key);
      if (value != null) {
        value = value.trim();
        if (value.length() == 0) {
          value = null;
        }
      }
    } catch (Exception e) {
      if (!failedKeys.contains(key)) {
        log.warn("Could not load config value for key (this message will only be logged once): " + key, e);
        failedKeys.add(key);
      }
    }

    return value;
  }
}
