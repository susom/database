/*
 * Copyright 2015 The Board of Trustees of The Leland Stanford Junior University.
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
 * Allow customization of the transaction behavior.
 */
public interface Transaction {
  /**
   * @return whether this code block has requested rollback upon a {@link Throwable}
   *         being thrown from the run method - this only reflects what was requested
   *         by calling {@link #setRollbackOnError(boolean)}, which is not necessarily
   *         what will actually happen
   */
  boolean isRollbackOnError();

  /**
   * Use this to request either "commit always" or "commit unless error" behavior.
   * This will have no effect if {@link #isRollbackOnly()} returns true.
   *
   * @param rollbackOnError true to rollback after errors; false to commit or rollback based on
   *                        the other settings
   * @see DatabaseProvider#transact(DbCodeTx)
   */
  void setRollbackOnError(boolean rollbackOnError);

  /**
   * @return whether this code block has requested unconditional rollback - this only
   *         reflects what was requested by calling {@link #setRollbackOnly(boolean)},
   *         which is not necessarily what will actually happen
   */
  boolean isRollbackOnly();

  /**
   * <p>If your code inside run() decides for some reason the transaction should rollback
   * rather than commit, use this method.</p>
   *
   * @param rollbackOnly true to request an unconditional rollback; false to commit or rollback based on
   *                     the other settings
   * @see DatabaseProvider#transact(DbCodeTx)
   */
  void setRollbackOnly(boolean rollbackOnly);
}
