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

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides explicit instrumentation functionality.
 *
 * @author garricko
 */
public class Metric {
  private final boolean enabled;
  private long startNanos;
  private long lastCheckpointNanos;
  private List<Checkpoint> checkpoints;

  private static class Checkpoint {
    String description;
    long durationNanos;

    Checkpoint(String description, long durationNanos) {
      this.description = description;
      this.durationNanos = durationNanos;
    }
  }

  public Metric(boolean enabled) {
    this.enabled = enabled;
    if (enabled) {
      checkpoints = new ArrayList<>();
      startNanos = System.nanoTime();
      lastCheckpointNanos = startNanos;
    }
  }

  public void checkpoint(String description) {
    if (enabled) {
      long currentCheckpointNanos = System.nanoTime();
      checkpoints.add(new Checkpoint(description, currentCheckpointNanos - lastCheckpointNanos));
      lastCheckpointNanos = currentCheckpointNanos;
    }
  }

  public long done(String description) {
    checkpoint(description);
    return done();
  }

  public long done() {
    if (enabled) {
      lastCheckpointNanos = System.nanoTime();
      return lastCheckpointNanos - startNanos;
    }
    return -1;
  }

  public String getMessage() {
    if (enabled) {
      StringBuilder buf = new StringBuilder();
      writeNanos(buf, lastCheckpointNanos - startNanos);
      if (!checkpoints.isEmpty()) {
        buf.append(" (");
        boolean first = true;
        for (Checkpoint checkpoint : checkpoints) {
          if (first) {
            first = false;
          } else {
            buf.append(',');
          }
          buf.append(checkpoint.description);
          buf.append('=');
          writeNanos(buf, checkpoint.durationNanos);
        }
        buf.append(')');
      }
      return buf.toString();
    }
    return "disabled";
  }

  private void writeNanos(StringBuilder buf, long nanos) {
    if (nanos < 0) {
      buf.append("-");
      nanos = -nanos;
    }
    String nanosStr = Long.toString(nanos);
    if (nanosStr.length() > 6) {
      buf.append(nanosStr.substring(0, nanosStr.length() - 6));
      buf.append('.');
      buf.append(nanosStr.substring(nanosStr.length() - 6));
    } else {
      buf.append("0.0000000".substring(0, 8 - nanosStr.length()));
      buf.append(nanosStr);
    }
    buf.append("ms");
  }
}
