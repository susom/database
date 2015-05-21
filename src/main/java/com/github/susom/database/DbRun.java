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
  private boolean rollbackOnError;
  private boolean rollbackOnly;

  /**
   * Implement this method to provide a block of code that uses the provided database
   * and is transacted. Whether the transaction will commit or rollback is typically
   * controlled by the code that invokes this method. This behavior can sometimes be
   * altered by calling the appropriate set*() methods on this class.
   *
   * <p>If a {@link Throwable} is thrown from this method, it will be caught, wrapped in
   * a DatabaseException (if it is not already one), and then propagated.</p>
   */
  public abstract void run(Provider<Database> db) throws Exception;

  /**
   * @return whether this code block has requested rollback upon a {@link Throwable}
   *         being thrown from the {@link #run(Provider)} method - this only
   *         reflects what was requested by calling {@link #setRollbackOnError(boolean)},
   *         which is not necessarily what will actually happen
   */
  public boolean isRollbackOnError() {
    return rollbackOnError;
  }

  /**
   * @deprecated use setRollbackOnError() instead
   */
  @Deprecated
  public void setRollbackOnException(boolean rollbackOnError) {
    this.rollbackOnError = rollbackOnError;
  }

  /**
   * By default the transaction behavior (whether commit() or rollback() is called)
   * after run() is specified by how this code block is invoked. For example, see
   * {@link DatabaseProvider#transactCommitOnly(DbRun)}, {@link DatabaseProvider#transactRollbackOnly(DbRun)},
   * and {@link DatabaseProvider#transactRollbackOnError(DbRun)}. Depending on the
   * context, you may be able to request different behavior using this method. See
   * documentation of these transact*() methods for details.
   *
   * @param rollbackOnError true to rollback after errors; false to commit or rollback based on
   *                        the other settings
   */
  public void setRollbackOnError(boolean rollbackOnError) {
    this.rollbackOnError = rollbackOnError;
  }

  /**
   * @return whether this code block has requested unconditional rollback - this only
   *         reflects what was requested by calling {@link #setRollbackOnly(boolean)},
   *         which is not necessarily what will actually happen
   */
  public boolean isRollbackOnly() {
    return rollbackOnly;
  }

  /**
   * <p>If your code inside run() decides for some reason the transaction should rollback
   * rather than commit, use this method.</p>
   *
   * <p>By default the transaction behavior (whether commit() or rollback() is called)
   * after run() is specified by how this code block is invoked. For example, see
   * {@link DatabaseProvider#transactCommitOnly(DbRun)}, {@link DatabaseProvider#transactRollbackOnly(DbRun)},
   * and {@link DatabaseProvider#transactRollbackOnError(DbRun)}. Depending on the
   * context, you may be able to request different behavior using this method. See
   * documentation of these transact*() methods for details.</p>
   *
   * @param rollbackOnly true to request an unconditional rollback; false to commit or rollback based on
   *                     the other settings
   */
  public void setRollbackOnly(boolean rollbackOnly) {
    this.rollbackOnly = rollbackOnly;
  }
}
