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

/**
 * Indicates something went wrong accessing the database. Most often this is
 * used to wrap SQLException to avoid declaring checked exceptions.
 *
 * @author garricko
 */
public class DatabaseException extends RuntimeException {
  public DatabaseException(String message) {
    super(message);
  }

  public DatabaseException(Throwable cause) {
    super(cause);
  }

  public DatabaseException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Wrap an exception with a DatabaseException, taking into account all known
   * subtypes such that we wrap subtypes in a matching type (so we don't obscure
   * the type available to catch clauses).
   *
   * @param message the new wrapping exception will have this message
   * @param cause the exception to be wrapped
   * @return the exception you should throw
   */
  public static DatabaseException wrap(String message, Throwable cause) {
    if (cause instanceof ConstraintViolationException) {
      return new ConstraintViolationException(message, cause);
    }
    return new DatabaseException(message, cause);
  }
}
