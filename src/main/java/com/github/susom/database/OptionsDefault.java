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

  @Override
  public boolean ignoreTransactionControl() {
    return false;
  }

  @Override
  public boolean allowConnectionAccess() {
    return false;
  }

  @Override
  public boolean isLogParameters() {
    return false;
  }

  @Override
  public boolean isDetailedExceptions() {
    return false;
  }

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

  @Override
  public boolean useDatePerAppOnly() {
    return false;
  }
}
