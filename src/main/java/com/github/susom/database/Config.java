package com.github.susom.database;

import java.math.BigDecimal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entry point for getting configuration parameters. This isn't intended as
 * a be-all, end-all configuration solution. Just a way of easily specifying
 * multiple read-only sources for configuration with a nice fluent syntax.
 *
 * @author garricko
 */
public interface Config {
  /**
   * Convenience method for fluent syntax.
   *
   * @return a builder for specifying from where configuration should be loaded
   */
  static @Nonnull ConfigFrom from() {
    return new ConfigFromImpl();
  }

  /**
   * @return a trimmed, non-empty string, or null
   */
  @Nullable String getString(@Nonnull String key);

  @Nonnull String getString(String key, @Nonnull String defaultValue);

  @Nullable Integer getInteger(@Nonnull String key);

  int getInteger(@Nonnull String key, int defaultValue);

  @Nullable Long getLong(@Nonnull String key);

  long getLong(@Nonnull String key, long defaultValue);

  @Nullable Float getFloat(@Nonnull String key);

  float getFloat(@Nonnull String key, float defaultValue);

  @Nullable Double getDouble(@Nonnull String key);

  double getDouble(@Nonnull String key, double defaultValue);

  @Nullable
  BigDecimal getBigDecimal(@Nonnull String key);

  @Nonnull BigDecimal getBigDecimal(String key, @Nonnull BigDecimal defaultValue);

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
}
