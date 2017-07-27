/*
 * Copyright 2014 The Board of Trustees of The Leland Stanford Junior University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.susom.database;

import java.util.Calendar;
import java.util.Date;

/**
 * Control various optional behavior for the database interactions.
 *
 * @author garricko
 */
public interface Options {
  /**
   * Control whether the Database object will allow calls to commitNow()
   * and rollbackNow(). By default it will throw exceptions if you try to
   * call those.
   */
  boolean allowTransactionControl();

  /**
   * Useful for testing code that explicitly controls transactions, and you
   * don't really want it to commit/rollback. Disabled by default, meaning
   * calls will be allowed or throw exceptions depending on allowTransctionControl().
   * The value of allowTranscationControl() has no affect if this returns true.
   */
  boolean ignoreTransactionControl();

  /**
   * Control whether the Database object will allow calls to underlyingConnection().
   * By default that method will throw an exception.
   */
  boolean allowConnectionAccess();

  /**
   * If this is false, log messages will look something like:
   *
   * <pre>
   *   ...select a from b where c=?
   * </pre>
   *
   * If this is true, log messages will look something like:
   *
   * <pre>
   *   ...select a from b where c=?|select a from b where c='abc'
   * </pre>
   *
   * @return true if parameter values should be logged along with SQL, false otherwise
   */
  boolean isLogParameters();

  /**
   * If true, text of the SQL and possibly parameter values (depending on @{#isLogParameters()})
   * will be included in exception messages. This can be very helpful for debugging, but poses
   * some disclosure risks.
   *
   * @return true to add possibly sensitive data in exception messages, false otherwise
   */
  boolean isDetailedExceptions();

  /**
   * In cases where exceptions are thrown, use this method to provide a common
   * code that will be included in the exception message and the log message
   * so they can be searched and correlated later.
   *
   * @return an arbitrary, fairly unique, speakable over the phone, without whitespace
   */
  String generateErrorCode();

  /**
   * Indicate whether to use the Blob functionality of the underlying database driver,
   * or whether to use setBytes() methods instead. Using Blobs is preferred, but is not
   * supported by all drivers.
   *
   * <p>The default behavior of this method is to delegate to flavor().useBytesForBlob(),
   * but it is provided on this interface so the behavior can be controlled.
   *
   * @return true to avoid using Blob functionality, false otherwise
   */
  boolean useBytesForBlob();

  /**
   * Indicate whether to use the Clob functionality of the underlying database driver,
   * or whether to use setString() methods instead. Using Clobs is preferred, but is not
   * supported by all drivers.
   *
   * <p>The default behavior of this method is to delegate to flavor().useStringForClob(),
   * but it is provided on this interface so the behavior can be controlled.
   *
   * @return true to avoid using Clob functionality, false otherwise
   */
  boolean useStringForClob();

  /**
   * Access compatibility information for the underlying database. The
   * Flavor class enumerates the known databases and tries to smooth over
   * some of the variations in features and syntax.
   */
  Flavor flavor();

  /**
   * The value returned by this method will be used for argDateNowPerApp() calls. It
   * may also be used for argDateNowPerDb() calls if you have enabled that.
   */
  Date currentDate();

  /**
   * Wherever argDateNowPerDb() is specified, use argDateNowPerApp() instead. This is
   * useful for testing purposes as you can use OptionsOverride to provide your
   * own system clock that will be used for time travel.
   */
  boolean useDatePerAppOnly();

  /**
   * This calendar will be used for conversions when storing and retrieving timestamps
   * from the database. By default this is the JVM default with TimeZone explicitly set
   * to GMT (so timestamps will be stored in the database as GMT).
   *
   * <p>It is strongly recommended to always run your database in GMT timezone, and
   * leave this set to the default.</p>
   *
   * <p>Behavior in releases 1.3 and prior was to use the JVM default TimeZone, and
   * this was not configurable.</p>
   */
  Calendar calendarForTimestamps();

  /**
   * The maximum number of characters to print in debug SQL for a given String type
   * insert/update/query parameter. If it exceeds this length, the parameter value
   * will be truncated at the max and a "..." will be appended. Note this affects
   * both {@code argString()} and {@code argClobString()} methods.
   */
  int maxStringLengthParam();
}
