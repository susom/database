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

package com.github.susom.database.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Allow a non-deterministic test to fail up to a certain number of times
 * and still pass.
 *
 * @author garricko
 */
public class Retryable implements TestRule {
  public Statement apply(Statement base, Description description) {
    return statement(base, description);
  }

  private Statement statement(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Retry retry = description.getAnnotation(Retry.class);

        if (retry == null || retry.maxTries() < 2) {
          base.evaluate();
          return;
        }

        Throwable error = null;
        for (int i = 0; i < retry.maxTries(); i++) {
          if (error != null) {
            error.printStackTrace();
          }
          try {
            base.evaluate();
            return;
          } catch (Throwable t) {
            error = t;
          }
        }
        throw error;
      }
    };
  }
}