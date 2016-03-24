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
  private boolean done;
  private long startNanos;
  private long lastCheckpointNanos;
  private List<Checkpoint> checkpoints;

  private static class Checkpoint {
    String description;
    long durationNanos;
    private final Object[] args;

    Checkpoint(String description, long durationNanos, Object... args) {
      this.description = description;
      this.durationNanos = durationNanos;
      this.args = args;
    }
  }

  /**
   * <p>Create a metric tracking object and start the nanosecond timer. Times
   * are obtained using {@code System.nanoTime()}. The canonical way to use
   * this looks something like this:
   * </p>
   *
   * <pre>
   *   Metric metric = new Metric(log.isDebugEnabled);
   *   ...
   *   metric.checkpoint("received");
   *   ...
   *   metric.checkpoint("processed");
   *   ...
   *   metric.done("sent");
   *   ...
   *   if (log.isDebugEnabled()) {
   *    log.debug("Processed: " + metric.getMessage());
   *   }
   * </pre>
   *
   * @param enabled {@code true} if timings will be taken, {@code false} to
   *                optimize out the time tracking
   */
  public Metric(boolean enabled) {
    this.enabled = enabled;
    if (enabled) {
      checkpoints = new ArrayList<>();
      startNanos = System.nanoTime();
      lastCheckpointNanos = startNanos;
    }
  }

  /**
   * Find out how many milliseconds have elapsed since this timer was started.
   *
   * @return the number of milliseconds elapsed, or -1 if {@code false} was
   *         passed in the constructor
   */
  public long elapsedMillis() {
    if (!enabled) {
      return -1;
    }
    return (System.nanoTime() - startNanos) / 1000000;
  }

  /**
   * Find out how many nanoseconds have elapsed since this timer was started.
   *
   * @return the number of nanoseconds elapsed, or -1 if {@code false} was
   *         passed in the constructor
   */
  public long elapsedNanos() {
    if (!enabled) {
      return -1;
    }
    return (System.nanoTime() - startNanos);
  }

  /**
   * Set a mark for timing. It is strongly recommended to use a short,
   * simple alphanumeric description. For example, "sent" or "didThat".
   * With this version you can provide additional arguments, such as
   * byte counts, which will be appended comma-separated within brackets.
   * For example, {@code checkpoint("sent", 25, 3)} will result in a
   * description "sent[25,3]". The string evaluation and concatenation is
   * lazy, and won't be done if this metric is disabled.
   *
   * @param description a label for this mark; may not be null; spaces
   *                    and tabs will be converted to underscores
   * @param args additional information to append to the description; will
   *             print "null" if null; evaluated with String.valueOf() lazily
   *             and sanitized of most non-alphanumeric characters
   */
  public void checkpoint(String description, Object... args) {
    if (enabled) {
      long currentCheckpointNanos = System.nanoTime();
      checkpoints.add(new Checkpoint(noTabsOrSpaces(description), currentCheckpointNanos - lastCheckpointNanos, args));
      lastCheckpointNanos = currentCheckpointNanos;
    }
  }

  /**
   * Set a final mark for timing and stop the timer. Once you call this
   * method, subsequent calls have no effect.
   *
   * @param description a label for this mark; may not be null; spaces
   *                    and tabs will be converted to underscores
   * @return time in nanoseconds from the start of this metric, or -1
   *         if {@code false} was passed in the constructor
   */
  public long done(String description, Object... args) {
    checkpoint(description, args);
    return done();
  }

  /**
   * Indicate we are done (stop the timer). Once you call this
   * method, subsequent calls have no effect.
   *
   * @return time in nanoseconds from the start of this metric, or -1
   *         if {@code false} was passed in the constructor
   */
  public long done() {
    if (enabled) {
      if (!done) {
        lastCheckpointNanos = System.nanoTime();
        done = true;
      }
      return lastCheckpointNanos - startNanos;
    }
    return -1;
  }

  /**
   * Construct and return a message based on the timing and checkpoints. This
   * will look like "123.456ms(checkpoint1=100.228ms,checkpoint2=23.228ms)"
   * without the quotes. There will be no spaces or tabs in the output.
   *
   * <p>This will automatically call the done() method to stop the timer if
   * you haven't already done so.</p>
   *
   * @return a string with timing information, or {@code "metricsDisabled"}
   *         if {@code false} was passed in the constructor.
   * @see #printMessage(StringBuilder)
   */
  public String getMessage() {
    if (enabled) {
      StringBuilder buf = new StringBuilder();
      printMessage(buf);
      return buf.toString();
    }
    return "metricsDisabled";
  }

  /**
   * Construct and print a message based on the timing and checkpoints. This
   * will look like "123.456ms(checkpoint1=100.228ms,checkpoint2=23.228ms)"
   * without the quotes. There will be no spaces or tabs in the output. A
   * value of {@code "metricsDisabled"} will be printed if {@code false} was
   * passed in the constructor.
   *
   * <p>This will automatically call the done() method to stop the timer if
   * you haven't already done so.</p>
   *
   * @param buf the message will be printed to this builder
   * @see #getMessage()
   */
  public void printMessage(StringBuilder buf) {
    if (enabled) {
      done();
      writeNanos(buf, lastCheckpointNanos - startNanos);
      if (!checkpoints.isEmpty()) {
        buf.append("(");
        boolean first = true;
        for (Checkpoint checkpoint : checkpoints) {
          if (first) {
            first = false;
          } else {
            buf.append(',');
          }
          buf.append(checkpoint.description);
          if (checkpoint.args != null && checkpoint.args.length > 0) {
            buf.append('[');
            boolean firstArg = true;
            for (Object o : checkpoint.args) {
              if (firstArg) {
                firstArg = false;
              } else {
                buf.append(',');
              }
              buf.append(sanitizeArg(String.valueOf(o)));
            }
            buf.append(']');
          }
          buf.append('=');
          writeNanos(buf, checkpoint.durationNanos);
        }
        buf.append(')');
      }
    } else {
      buf.append("metricsDisabled");
    }
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
      buf.append(nanosStr.substring(nanosStr.length() - 6, nanosStr.length() - 3));
    } else {
      buf.append("0.0000000".substring(0, 8 - Math.max(nanosStr.length(), 4)));
      if (nanosStr.length() > 3) {
        buf.append(nanosStr.substring(0, nanosStr.length() - 3));
      }
    }
    buf.append("ms");
  }

  private String noTabsOrSpaces(String s) {
    return s.replace(' ', '_').replace('\t', '_');
  }

  private String sanitizeArg(String s) {
    return s.replaceAll("[^\\p{Alnum}_.\\-\\+]", "*");
  }
}
