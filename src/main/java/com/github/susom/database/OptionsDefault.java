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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Control various optional behavior for the database interactions.
 *
 * @author garricko
 */
public class OptionsDefault implements Options {
  private final Flavor flavor;

  public OptionsDefault(Flavor flavor) {
    this.flavor = flavor;
  }

  @Override
  public boolean allowTransactionControl() {
    return false;
  }

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
  @Override
  public boolean isLogParameters() {
    return false;
  }

  /**
   * If true, text of the SQL and possibly parameter values (depending on @{#isLogParameters()})
   * will be included in exception messages. This can be very helpful for debugging, but poses
   * some disclosure risks.
   *
   * @return true to add possibly sensitive data in exception messages, false otherwise
   */
  @Override
  public boolean isDetailedExceptions() {
    return false;
  }

  /**
   * In cases where exceptions are thrown, use this method to provide a common
   * code that will be included in the exception message and the log message
   * so they can be searched and correlated later.
   *
   * @return an arbitrary, fairly unique, speakable over the phone, without whitespace
   */
  @Override
  public String generateErrorCode() {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:H:m:s");
    return sdf.format(new Date()) + "-" + Math.abs(new Random().nextInt());
  }

  @Override
  public boolean useBytesForBlob() {
    return flavor().useBytesForBlob();
  }

  @Override
  public boolean useStringForClob() {
    return flavor().useStringForClob();
  }

  @Override
  public Flavor flavor() {
    return flavor;
  }

  @Override
  public Date currentDate() {
    return new Date();
  }
}
