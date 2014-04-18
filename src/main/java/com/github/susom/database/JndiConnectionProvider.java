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
