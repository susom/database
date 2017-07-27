/*
 * Copyright 2017 The Board of Trustees of The Leland Stanford Junior University.
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

import java.io.StringReader;

/**
 * This class exists to distinguish cases where we are mapping String to Reader
 * internally, but want to be able to know they really started as a String (and
 * be able to get the String back for things like logging).
 *
 * @author garricko
 */
final class InternalStringReader extends StringReader {
  private String s;

  InternalStringReader(String s) {
    super(s);
    this.s = s;
  }

  String getString() {
    return s;
  }
}
