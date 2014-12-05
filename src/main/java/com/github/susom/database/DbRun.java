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

import javax.inject.Provider;

/**
 * Abstract base class for a block of runnable code using a transacted Database.
 *
 * @author garricko
 */
public abstract class DbRun {
  private boolean rollbackOnException;
  private boolean rollbackOnly;

  /**
   * Implement this method to provide a block of code that uses the provided database
   * and is transacted. By default the transaction will be committed after calling this
   * method, regardless of whether it returns or throws an exception. This behavior can
   * be controlled by calling the appropriate set*() methods on this class.
   *
   * <p>If a checked exception is thrown from this method, it will be caught, wrapped in
   * a DatabaseException, and then propagated.</p>
   */
  public abstract void run(Provider<Database> db) throws Exception;

  public boolean isRollbackOnError() {
    return rollbackOnException;
  }

  /**
   * By default the transaction will commit after run() regardless of whether an
   * exception was thrown. This method allows you to change that behavior so the
   * transaction will be rolled back.
   *
   * @param rollbackOnException true to rollback after exceptions; false otherwise
   */
  public void setRollbackOnException(boolean rollbackOnException) {
    this.rollbackOnException = rollbackOnException;
  }

  public boolean isRollbackOnly() {
    return rollbackOnly;
  }

  /**
   * If your code inside run() decides for some reason the transaction should rollback
   * rather than commit, use this method.
   *
   * @param rollbackOnly true to force a rollback; false to commit or rollback based on
   *                     the other settings (default of commit, or setRollbackOnException())
   */
  public void setRollbackOnly(boolean rollbackOnly) {
    this.rollbackOnly = rollbackOnly;
  }
}
