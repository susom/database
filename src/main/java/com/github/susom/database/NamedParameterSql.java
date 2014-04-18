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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convenience class to allow use of (:mylabel) for SQL parameters rather than strictly
 * positional (?) parameters. This doesn't do any smart parsing of the SQL, it is just
 * looking for ':' characters. If the SQL needs to include an actual ':' character, use
 * double colon '::', which will be replaced with a single ':'.
 *
 * @author garricko
 */
public class NamedParameterSql {
  private final String sqlToExecute;
  private final String[] argNames;

  public NamedParameterSql(String sql) {
    StringBuilder newSql = new StringBuilder(sql.length());
    List<String> argNames = new ArrayList<>();
    int searchIndex = 0;
    while (searchIndex < sql.length()) {
      int nextColonIndex = sql.indexOf(':', searchIndex);

      if (nextColonIndex < 0) {
        newSql.append(sql.substring(searchIndex));
        break;
      }

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
      newSql.append('?');
      String paramName = sql.substring(nextColonIndex + 1, endOfNameIndex);
      argNames.add(paramName);
      searchIndex = endOfNameIndex;
    }

    this.sqlToExecute = newSql.toString();
    this.argNames = argNames.toArray(new String[argNames.size()]);
  }

  public String getSqlToExecute() {
    return sqlToExecute;
  }

  public Object[] toArgs(Map<String, Object> nameToArg) {
    Object[] args = new Object[argNames.length];
    for (int i = 0; i < argNames.length; i++) {
      if (nameToArg.containsKey(argNames[i])) {
        args[i] = nameToArg.get(argNames[i]);
      } else {
        throw new DatabaseException("The SQL requires parameter '" + argNames[i] + "' but no value was provided");
      }
    }
    // Sanity check number of arguments to provide a better error message
    if (nameToArg.size() > args.length) {
      Set<String> unusedNames = new HashSet<>(nameToArg.keySet());
      unusedNames.removeAll(Arrays.asList(argNames));
      throw new DatabaseException("These named parameters do not exist in the query: " + unusedNames);
    }
    return args;
  }
}
