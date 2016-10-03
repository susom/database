package com.github.susom.database;

import java.math.BigDecimal;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entry point for getting configuration parameters. This isn't intended as
 * a be-all, end-all configuration solution. Just a way of easily specifying
 * multiple read-only sources for configuration with a nice fluent syntax.
 *
 * @author garricko
 */
public interface Config extends Function<String, String>, Supplier<Config> {
  /**
   * Convenience method for fluent syntax.
   *
   * @return a builder for specifying from where configuration should be loaded
   */
  static @Nonnull ConfigFrom from() {
    return new ConfigFromImpl();
  }

  // TODO add: String originalKey(String key) to find out the key before prefixing or other manipulation

  /**
   * @return a trimmed, non-empty string, or null
   */
  @Nullable String getString(@Nonnull String key);

  /**
   * @return a trimmed, non-empty string
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  @Nonnull String getStringOrThrow(@Nonnull String key);

  @Nonnull String getString(String key, @Nonnull String defaultValue);

  /**
   * Same as {@link #getString(String)}. Useful for passing configs around
   * without static dependencies.
   */
  @Override
  default String apply(String key) {
    return getString(key);
  }

  @Override
  default Config get() { return this; }

  @Nullable Integer getInteger(@Nonnull String key);

  int getInteger(@Nonnull String key, int defaultValue);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  int getIntegerOrThrow(@Nonnull String key);

  @Nullable Long getLong(@Nonnull String key);

  long getLong(@Nonnull String key, long defaultValue);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  long getLongOrThrow(@Nonnull String key);

  @Nullable Float getFloat(@Nonnull String key);

  float getFloat(@Nonnull String key, float defaultValue);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  float getFloatOrThrow(@Nonnull String key);

  @Nullable Double getDouble(@Nonnull String key);

  double getDouble(@Nonnull String key, double defaultValue);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  double getDoubleOrThrow(@Nonnull String key);

  @Nullable
  BigDecimal getBigDecimal(@Nonnull String key);

  @Nonnull BigDecimal getBigDecimal(String key, @Nonnull BigDecimal defaultValue);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  @Nonnull BigDecimal getBigDecimalOrThrow(String key);

  /**
   * Read a boolean value from the configuration. The value is not case-sensitivie,
   * and may be either true/false or yes/no. If no value was provided or an invalid
   * value is provided, false will be returned.
   */
  boolean getBooleanOrFalse(@Nonnull String key);

  /**
   * Read a boolean value from the configuration. The value is not case-sensitivie,
   * and may be either true/false or yes/no. If no value was provided or an invalid
   * value is provided, true will be returned.
   */
  boolean getBooleanOrTrue(@Nonnull String key);

  /**
   * @throws ConfigMissingException if no value could be read for the specified key
   */
  boolean getBooleanOrThrow(@Nonnull String key);

  /**
   * Show where configuration is coming from. This is useful to drop in your logs
   * for troubleshooting.
   */
  String sources();
}
