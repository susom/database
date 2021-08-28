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

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Convenience for conditional SQL generation.
 */
public class When {
  private String chosen;
  private final Flavor actualFlavor;

  public When(Flavor actualFlavor) {
    this.actualFlavor = actualFlavor;
  }

  @Nonnull
  public When oracle(@Nonnull String sql) {
    if (actualFlavor == Flavor.oracle) {
      chosen = sql;
    }
    return this;
  }

  @Nonnull
  public When derby(@Nonnull String sql) {
    if (actualFlavor == Flavor.derby) {
      chosen = sql;
    }
    return this;
  }

  @Nonnull
  public When postgres(@Nonnull String sql) {
    if (actualFlavor == Flavor.postgresql) {
      chosen = sql;
    }
    return this;
  }
  
  @Nonnull
  public When sqlserver(@Nonnull String sql){
	  if (actualFlavor == Flavor.sqlserver) {
		  chosen = sql;
	  }
	  return this;
  }

  @Nonnull
  public String other(@Nonnull String sql) {
    if (chosen == null) {
      chosen = sql;
    }
    return chosen;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    When when = (When) o;
    return Objects.equals(chosen, when.chosen) && actualFlavor == when.actualFlavor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(chosen, actualFlavor);
  }

  @Override
  public String toString() {
    return chosen == null ? "" : chosen;
  }
}
