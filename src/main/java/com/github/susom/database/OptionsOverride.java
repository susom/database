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

import java.util.Date;

/**
 * Base class for selectively overriding another Options object.
 *
 * @author garricko
 */
public class OptionsOverride implements Options {
  private Options parent;

  /**
   * Defer to OptionsDefault for anything that is not specified, and use postgresql flavor.
   */
  public OptionsOverride() {
    parent = new OptionsDefault(Flavor.postgresql);
  }

  /**
   * Defer to OptionsDefault for anything that is not specified, using the specified flavor.
   */
  public OptionsOverride(Flavor flavor) {
    parent = new OptionsDefault(flavor);
  }

  public void setParent(Options parent) {
    this.parent = parent;
  }

  public OptionsOverride withParent(Options parent) {
    this.parent = parent;
    return this;
  }

  @Override
  public boolean allowTransactionControl() {
    return parent.allowTransactionControl();
  }

  @Override
  public boolean ignoreTransactionControl() {
    return parent.ignoreTransactionControl();
  }

  @Override
  public boolean allowConnectionAccess() {
    return parent.allowConnectionAccess();
  }

  @Override
  public boolean isLogParameters() {
    return parent.isLogParameters();
  }

  @Override
  public boolean isDetailedExceptions() {
    return parent.isDetailedExceptions();
  }

  @Override
  public String generateErrorCode() {
    return parent.generateErrorCode();
  }

  @Override
  public boolean useBytesForBlob() {
    return parent.useBytesForBlob();
  }

  @Override
  public boolean useStringForClob() {
    return parent.useStringForClob();
  }

  @Override
  public Flavor flavor() {
    return parent.flavor();
  }

  @Override
  public Date currentDate() {
    return parent.currentDate();
  }

  @Override
  public boolean useDatePerAppOnly() {
    return parent.useDatePerAppOnly();
  }
}
