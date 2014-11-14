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

import org.jetbrains.annotations.NotNull;

/**
 * Convenience for conditional SQL generation.
 */
public class When {
  private String chosen;
  private Flavor actualFlavor;

  public When(Flavor actualFlavor) {
    this.actualFlavor = actualFlavor;
  }

  @NotNull
  public When oracle(@NotNull String sql) {
    if (actualFlavor == Flavor.oracle) {
      chosen = sql;
    }
    return this;
  }

  @NotNull
  public When derby(@NotNull String sql) {
    if (actualFlavor == Flavor.derby) {
      chosen = sql;
    }
    return this;
  }

  @NotNull
  public When postgres(@NotNull String sql) {
    if (actualFlavor == Flavor.postgresql) {
      chosen = sql;
    }
    return this;
  }

  @NotNull
  public String other(@NotNull String sql) {
    if (chosen == null) {
      chosen = sql;
    }
    return chosen;
  }

  @Override
  public int hashCode() {
    return other("").hashCode();
  }

  @Override
  public String toString() {
    return other("");
  }
}
