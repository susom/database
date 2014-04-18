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
import java.sql.DriverManager;
import java.util.Properties;

import javax.inject.Provider;

/**
 * Connection provider that just delegates to DriverManager.
 *
 * @author garricko
 */
public class DriverManagerConnectionProvider implements Provider<Connection> {
  private final String url;
  private final String user;
  private final String password;
  private final Properties info;

  public DriverManagerConnectionProvider(String url) {
    this.url = url;
    user = null;
    password = null;
    info = null;
  }

  public DriverManagerConnectionProvider(String url, Properties info) {
    this.url = url;
    user = null;
    password = null;
    this.info = info;
  }

  public DriverManagerConnectionProvider(String url, String user, String password) {
    this.url = url;
    this.user = user;
    this.password = password;
    info = null;
  }

  public Connection get() {
    try {
      if (info != null) {
        return DriverManager.getConnection(url, info);
      } else if (user != null) {
        return DriverManager.getConnection(url, user, password);
      }
      return DriverManager.getConnection(url);
    } catch (Exception e) {
      throw new DatabaseException("Unable to obtain a connection from DriverManager", e);
    }
  }
}
