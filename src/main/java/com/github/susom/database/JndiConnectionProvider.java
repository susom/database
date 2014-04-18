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

import java.sql.Connection;

import javax.inject.Provider;
import javax.naming.Context;
import javax.sql.DataSource;

/**
 * Simple lookup utility to get a DataSource and subsequent Connection from a JNDI Context.
 *
 * @author garricko
 */
public class JndiConnectionProvider implements Provider<Connection> {
  private Context context;
  private String lookupKey;

  public JndiConnectionProvider(Context context, String lookupKey) {
    this.context = context;
    this.lookupKey = lookupKey;
  }

  public Connection get() {
    DataSource ds;
    try {
      ds = (DataSource) context.lookup(lookupKey);
    } catch (Exception e) {
      throw new DatabaseException("Unable to locate the DataSource in JNDI using key " + lookupKey, e);
    }
    try {
      return ds.getConnection();
    } catch (Exception e) {
      throw new DatabaseException("Unable to obtain a connection from JNDI DataSource " + lookupKey, e);
    }
  }
}
