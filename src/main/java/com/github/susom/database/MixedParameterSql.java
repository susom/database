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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convenience class to allow use of (:mylabel) for SQL parameters in addition to
 * positional (?) parameters.
 *
 * <p>By default this uses "smart" parsing, which is aware of ordinary SQL syntax. A
 * ':' or '?' character that appears inside a single-quoted string literal ('...'),
 * a double-quoted identifier ("..."), a line comment (-- ...) or a block comment
 * (/* ... *&#47;) is treated as regular SQL text and does not need to be escaped.
 * PostgreSQL-style casts (::type) are recognized and left untouched. Only a '?' or
 * ':name' occurring in ordinary SQL is treated as a bind variable.</p>
 *
 * <p>The legacy behavior can be requested by passing {@code useSmartParsing=false}
 * to the constructor. In that mode no smart parsing is done, and the SQL is simply
 * scanned for ':' and '?' characters. If the SQL needs to include an actual ':' or
 * '?' character in that mode, use two of them ('::' or '??'), and they will be
 * replaced with a single ':' or '?'.</p>
 *
 * @author garricko
 */
public class MixedParameterSql {
  private final String sqlToExecute;
  private final Object[] args;

  /**
   * Parse the SQL using the default "smart" parsing. Equivalent to calling
   * {@link #MixedParameterSql(String, List, Map, boolean)} with {@code true}.
   */
  public MixedParameterSql(String sql, List<Object> positionalArgs, Map<String, Object> nameToArg) {
    this(sql, positionalArgs, nameToArg, true);
  }

  /**
   * @param useSmartParsing if true, ':' and '?' inside string literals, quoted
   *                        identifiers and comments are ignored (and do not need to
   *                        be escaped); if false, the legacy escape-by-doubling
   *                        behavior is used
   */
  public MixedParameterSql(String sql, List<Object> positionalArgs, Map<String, Object> nameToArg,
                           boolean useSmartParsing) {
    if (positionalArgs == null) {
      positionalArgs = new ArrayList<>();
    }
    if (nameToArg == null) {
      nameToArg = new HashMap<>();
    }

    StringBuilder newSql = new StringBuilder(sql.length());
    List<String> argNamesList = new ArrayList<>();
    List<String> rewrittenArgs = new ArrayList<>();
    List<Object> argsList = new ArrayList<>();
    int currentPositionalArg = useSmartParsing
        ? parseSmart(sql, positionalArgs, nameToArg, newSql, argsList, argNamesList, rewrittenArgs)
        : parseLegacy(sql, positionalArgs, nameToArg, newSql, argsList, argNamesList, rewrittenArgs);

    this.sqlToExecute = newSql.toString();
    args = argsList.toArray(new Object[argsList.size()]);

    // Sanity check number of arguments to provide a better error message
    if (currentPositionalArg != positionalArgs.size()) {
      throw new DatabaseException("Wrong number of positional parameters were provided (expected: "
          + currentPositionalArg + ", actual: " + positionalArgs.size() + ")");
    }
    if (nameToArg.size() > args.length - Math.max(0, positionalArgs.size() - 1) + rewrittenArgs.size()) {
      Set<String> unusedNames = new HashSet<>(nameToArg.keySet());
      unusedNames.removeAll(argNamesList);
      unusedNames.removeAll(rewrittenArgs);
      if (!unusedNames.isEmpty()) {
        throw new DatabaseException("These named parameters do not exist in the query: " + unusedNames);
      }
    }
  }

  /**
   * Context-aware parsing that skips over string literals, quoted identifiers and
   * comments so ':' and '?' characters within them are left untouched.
   *
   * @return the number of positional parameters consumed
   */
  private int parseSmart(String sql, List<Object> positionalArgs, Map<String, Object> nameToArg,
                         StringBuilder newSql, List<Object> argsList, List<String> argNamesList,
                         List<String> rewrittenArgs) {
    int currentPositionalArg = 0;
    int length = sql.length();
    int i = 0;
    while (i < length) {
      char c = sql.charAt(i);
      switch (c) {
      case '$':
        // PostgreSQL dollar-quoting: $$...$$ or $tag$...$tag$
        if (i + 1 < length && (sql.charAt(i + 1) == '$' || Character.isLetter(sql.charAt(i + 1)) || sql.charAt(i + 1) == '_')) {
          int tagEnd = i + 1;
          while (tagEnd < length && sql.charAt(tagEnd) != '$') {
            tagEnd++;
          }
          if (tagEnd < length) {
            String tag = sql.substring(i, tagEnd + 1); // e.g. "$$" or "$tag$"
            int bodyStart = tagEnd + 1;
            int closeIndex = sql.indexOf(tag, bodyStart);
            if (closeIndex >= 0) {
              newSql.append(sql, i, closeIndex + tag.length());
              i = closeIndex + tag.length();
            } else {
              // Unterminated dollar-quoted string - copy to end
              newSql.append(sql, i, length);
              i = length;
            }
          } else {
            newSql.append(c);
            i++;
          }
        } else {
          newSql.append(c);
          i++;
        }
        break;
      case '[':
        // SQL Server bracketed identifier: [...] (] is escaped as ]])
        i = appendBracketedIdentifier(sql, i, newSql);
        break;
      case '\'':
        // Single-quoted string literal (with '' as an embedded quote)
        i = appendQuoted(sql, i, '\'', newSql);
        break;
      case '"':
        // Double-quoted identifier (with "" as an embedded quote)
        i = appendQuoted(sql, i, '"', newSql);
        break;
      case '-':
        if (i + 1 < length && sql.charAt(i + 1) == '-') {
          i = appendLineComment(sql, i, newSql);
        } else {
          newSql.append(c);
          i++;
        }
        break;
      case '/':
        if (i + 1 < length && sql.charAt(i + 1) == '*') {
          i = appendBlockComment(sql, i, newSql);
        } else {
          newSql.append(c);
          i++;
        }
        break;
      case '?':
        currentPositionalArg = appendPositionalParam(newSql, currentPositionalArg, positionalArgs, argsList);
        i++;
        break;
      case ':':
        if (i + 1 < length && sql.charAt(i + 1) == ':') {
          // PostgreSQL cast operator (::) - leave it untouched
          newSql.append("::");
          i += 2;
        } else if (i + 1 < length && Character.isJavaIdentifierPart(sql.charAt(i + 1))) {
          // Named parameter (":foo")
          int endOfNameIndex = i + 1;
          while (endOfNameIndex < length && Character.isJavaIdentifierPart(sql.charAt(endOfNameIndex))) {
            endOfNameIndex++;
          }
          appendNamedParam(newSql, sql.substring(i + 1, endOfNameIndex), nameToArg, argsList, argNamesList,
              rewrittenArgs);
          i = endOfNameIndex;
        } else {
          // A lone ':' that is not a parameter (e.g. an operator) - leave it as-is
          newSql.append(c);
          i++;
        }
        break;
      default:
        newSql.append(c);
        i++;
      }
    }
    return currentPositionalArg;
  }

  /**
   * Legacy parsing that treats every ':' and '?' as a parameter marker, and relies
   * on doubling ('::' or '??') to escape a literal ':' or '?'.
   *
   * @return the number of positional parameters consumed
   */
  private int parseLegacy(String sql, List<Object> positionalArgs, Map<String, Object> nameToArg,
                          StringBuilder newSql, List<Object> argsList, List<String> argNamesList,
                          List<String> rewrittenArgs) {
    int searchIndex = 0;
    int currentPositionalArg = 0;
    while (searchIndex < sql.length()) {
      int nextColonIndex = sql.indexOf(':', searchIndex);
      int nextQmIndex = sql.indexOf('?', searchIndex);

      if (nextColonIndex < 0 && nextQmIndex < 0) {
        newSql.append(sql.substring(searchIndex));
        break;
      }

      if (nextColonIndex >= 0 && (nextQmIndex == -1 || nextColonIndex < nextQmIndex)) {
        // The next parameter we found is a named parameter (":foo")
        if (nextColonIndex > sql.length() - 2) {
          // Probably illegal sql, but handle boundary condition
          break;
        }

        // Allow :: as escape for :
        if (sql.charAt(nextColonIndex + 1) == ':') {
          newSql.append(sql.substring(searchIndex, nextColonIndex + 1));
          searchIndex = nextColonIndex + 2;
          continue;
        }

        int endOfNameIndex = nextColonIndex + 1;
        while (endOfNameIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(endOfNameIndex))) {
          endOfNameIndex++;
        }
        newSql.append(sql.substring(searchIndex, nextColonIndex));
        appendNamedParam(newSql, sql.substring(nextColonIndex + 1, endOfNameIndex), nameToArg, argsList,
            argNamesList, rewrittenArgs);
        searchIndex = endOfNameIndex;
      } else {
        // The next parameter we found is a positional parameter ("?")

        // Allow ?? as escape for ?
        if (nextQmIndex < sql.length() - 1 && sql.charAt(nextQmIndex + 1) == '?') {
          newSql.append(sql.substring(searchIndex, nextQmIndex + 1));
          searchIndex = nextQmIndex + 2;
          continue;
        }

        newSql.append(sql.substring(searchIndex, nextQmIndex));
        currentPositionalArg = appendPositionalParam(newSql, currentPositionalArg, positionalArgs, argsList);
        searchIndex = nextQmIndex + 1;
      }
    }
    return currentPositionalArg;
  }

  /**
   * Emit a named parameter as a '?' placeholder (or its rewritten SQL) and record
   * its value/name for binding.
   */
  private void appendNamedParam(StringBuilder newSql, String paramName, Map<String, Object> nameToArg,
                                List<Object> argsList, List<String> argNamesList, List<String> rewrittenArgs) {
    boolean secretParam = paramName.startsWith("secret");
    Object arg = nameToArg.get(paramName);
    if (arg instanceof RewriteArg) {
      newSql.append(((RewriteArg) arg).sql);
      rewrittenArgs.add(paramName);
    } else {
      newSql.append('?');
      if (nameToArg.containsKey(paramName)) {
        argsList.add(secretParam ? new SecretArg(arg): arg);
      } else {
        throw new DatabaseException("The SQL requires parameter ':" + paramName + "' but no value was provided");
      }
      argNamesList.add(paramName);
    }
  }

  /**
   * Emit a positional parameter as a '?' placeholder (or its rewritten SQL) and
   * record its value for binding.
   *
   * @return the index of the next positional parameter to consume
   */
  private int appendPositionalParam(StringBuilder newSql, int currentPositionalArg,
                                    List<Object> positionalArgs, List<Object> argsList) {
    if (currentPositionalArg >= positionalArgs.size()) {
      throw new DatabaseException("Not enough positional parameters (" + positionalArgs.size() + ") were provided");
    }
    if (positionalArgs.get(currentPositionalArg) instanceof RewriteArg) {
      newSql.append(((RewriteArg) positionalArgs.get(currentPositionalArg)).sql);
    } else {
      newSql.append('?');
      argsList.add(positionalArgs.get(currentPositionalArg));
    }
    return currentPositionalArg + 1;
  }

  /**
   * Copy a SQL Server bracketed identifier ({@code [...]}), including the surrounding
   * brackets, verbatim into the output. A {@code ]]} inside the identifier is treated
   * as an escaped {@code ]}, not a terminator.
   *
   * @param start index of the opening '['
   * @return the index immediately after the closing ']' (or the end of the SQL if
   *         the identifier is not terminated)
   */
  private static int appendBracketedIdentifier(String sql, int start, StringBuilder newSql) {
    int length = sql.length();
    newSql.append('[');
    int i = start + 1;
    while (i < length) {
      char c = sql.charAt(i);
      if (c == ']') {
        if (i + 1 < length && sql.charAt(i + 1) == ']') {
          // Escaped ]] - part of the identifier
          newSql.append("]]");
          i += 2;
          continue;
        }
        // Closing bracket
        newSql.append(']');
        return i + 1;
      }
      newSql.append(c);
      i++;
    }
    // Unterminated - everything remaining has already been copied
    return i;
  }

  /**
   * Copy a quoted region (string literal or quoted identifier), including the
   * surrounding quote characters, verbatim into the output. A doubled quote
   * ({@code ''} or {@code ""}) is treated as an embedded quote, not a terminator.
   *
   * @param start index of the opening quote
   * @return the index immediately after the closing quote (or the end of the SQL if
   *         the quote is not terminated)
   */
  private static int appendQuoted(String sql, int start, char quote, StringBuilder newSql) {
    int length = sql.length();
    newSql.append(quote);
    int i = start + 1;
    while (i < length) {
      char c = sql.charAt(i);
      if (c == quote) {
        if (i + 1 < length && sql.charAt(i + 1) == quote) {
          // Embedded (doubled) quote - part of the quoted text
          newSql.append(quote).append(quote);
          i += 2;
          continue;
        }
        // Closing quote
        newSql.append(quote);
        return i + 1;
      }
      newSql.append(c);
      i++;
    }
    // Unterminated quote - everything remaining has already been copied
    return i;
  }

  /**
   * Copy a line comment ({@code -- ...}) verbatim into the output, up to but not
   * including the terminating newline.
   *
   * @param start index of the first '-'
   * @return the index of the newline that ends the comment (or the end of the SQL)
   */
  private static int appendLineComment(String sql, int start, StringBuilder newSql) {
    int length = sql.length();
    int i = start;
    while (i < length && sql.charAt(i) != '\n') {
      newSql.append(sql.charAt(i));
      i++;
    }
    return i;
  }

  /**
   * Copy a block comment ({@code /}{@code * ... *}{@code /}) verbatim into the
   * output. Nested block comments (as supported by PostgreSQL) are handled.
   *
   * @param start index of the opening '/'
   * @return the index immediately after the closing "*&#47;" (or the end of the SQL
   *         if the comment is not terminated)
   */
  private static int appendBlockComment(String sql, int start, StringBuilder newSql) {
    int length = sql.length();
    newSql.append("/*");
    int i = start + 2;
    int depth = 1;
    while (i < length && depth > 0) {
      if (i + 1 < length && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        newSql.append("/*");
        i += 2;
        depth++;
      } else if (i + 1 < length && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        newSql.append("*/");
        i += 2;
        depth--;
      } else {
        newSql.append(sql.charAt(i));
        i++;
      }
    }
    return i;
  }

  public String getSqlToExecute() {
    return sqlToExecute;
  }

  public Object[] getArgs() {
    return args;
  }

  public static class RewriteArg {
    private final String sql;

    public RewriteArg(String sql) {
      this.sql = sql;
    }
  }

  static class SecretArg {
    private final Object arg;

    SecretArg(Object arg) {
      this.arg = arg;
    }

    Object getArg() {
      return arg;
    }

    @Override
    public String toString() {
      return "<secret>";
    }
  }
}
