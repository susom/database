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

/**
 * Enumeration of supported databases with various compatibility settings.
 *
 * @author garricko
 */
public enum Flavor {
  generic,
  derby,
  oracle {
    @Override
    public String typeLong() {
      return "numeric(19)";
    }

    @Override
    public String typeStringVar(int bytes) {
      return "varchar2(" + bytes + ")";
    }
  },
  postgresql {
    @Override
    public String typeClob() {
      return "text";
    }

    @Override
    public String typeBlob() {
      return "bytea";
    }

    @Override
    public boolean useStringForClob() {
      return true;
    }

    @Override
    public boolean useBytesForBlob() {
      return true;
    }
  };

  public String typeInteger() {
    return "integer";
  }

  public String typeLong() {
    return "bigint";
  }

  public String typeFloat() {
    return "real";
  }

  public String typeDouble() {
    return "double";
  }

  public String typeBigDecimal(int size, int precision) {
    return "numeric(" + size + "," + precision + ")";
  }

  public String typeStringVar(int bytes) {
    return "varchar(" + bytes + ")";
  }

  public String typeStringFixed(int bytes) {
    return "char(" + bytes + ")";
  }

  public String typeClob() {
    return "clob";
  }

  public String typeBlob() {
    return "blob";
  }

  public String typeDate() {
    return "timestamp";
  }

  public boolean useStringForClob() {
    return false;
  }

  public boolean useBytesForBlob() {
    return false;
  }

  public static Flavor fromJdbcUrl(String url) {
    if (url.startsWith("jdbc:postgresql:")) {
      return postgresql;
    } else if (url.startsWith("jdbc:oracle:")) {
      return oracle;
    } else if (url.startsWith("jdbc:derby:")) {
      return derby;
    } else {
      return generic;
    }
  }
}
