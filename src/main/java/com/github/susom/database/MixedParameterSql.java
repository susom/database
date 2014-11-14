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
 * positional (?) parameters. This doesn't do any smart parsing of the SQL, it is just
 * looking for ':' and '?' characters. If the SQL needs to include an actual ':' or '?'
 * character, use two of them ('::' or '??'), and they will be replaced with a
 * single ':' or '?'.
 *
 * @author garricko
 */
public class MixedParameterSql {
  private final String sqlToExecute;
  private final Object[] args;

  public MixedParameterSql(String sql, List<Object> positionalArgs, Map<String, Object> nameToArg) {
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
        String paramName = sql.substring(nextColonIndex + 1, endOfNameIndex);
        if (nameToArg.get(paramName) instanceof RewriteArg) {
          newSql.append(((RewriteArg) nameToArg.get(paramName)).sql);
          rewrittenArgs.add(paramName);
        } else {
          newSql.append('?');
          if (nameToArg.containsKey(paramName)) {
            argsList.add(nameToArg.get(paramName));
          } else {
            throw new DatabaseException("The SQL requires parameter ':" + paramName + "' but no value was provided");
          }
          argNamesList.add(paramName);
        }
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
        if (currentPositionalArg >= positionalArgs.size()) {
          throw new DatabaseException("Not enough positional parameters (" + positionalArgs.size() + ") were provided");
        }
        if (positionalArgs.get(currentPositionalArg) instanceof RewriteArg) {
          newSql.append(((RewriteArg) positionalArgs.get(currentPositionalArg)).sql);
        } else {
          newSql.append('?');
          argsList.add(positionalArgs.get(currentPositionalArg));
        }
        currentPositionalArg++;
        searchIndex = nextQmIndex + 1;
      }
    }

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
}
