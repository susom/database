/*
 * Copyright 2016 The Board of Trustees of The Leland Stanford Junior University.
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

/**
 * Indicates that a configuration value is present but not in a usable format.
 * For example, if the value must be an integer and the configuration value
 * is a non-numeric string.
 *
 * @author garricko
 */
public class ConfigInvalidException extends DatabaseException {
  public ConfigInvalidException(String message) {
    super(message);
  }

  public ConfigInvalidException(Throwable cause) {
    super(cause);
  }

  public ConfigInvalidException(String message, Throwable cause) {
    super(message, cause);
  }
}
