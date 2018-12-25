/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.legacydata;

import java.util.Properties;


/**
 * This data class holds Spark environment data (Spark properties, JVM properties and etc.)
 */
public class SparkEnvironmentData {
  private final Properties _sparkProperties;
  private final Properties _systemProperties;
  private final Properties _jvmInformations;
  private final Properties _classPathEntries;

  public SparkEnvironmentData() {
    _sparkProperties = new Properties();
    _systemProperties = new Properties();
    _jvmInformations = new Properties();
    _classPathEntries = new Properties();
  }

  public void addSparkProperty(String key, String value) {
    _sparkProperties.put(key, value);
  }

  public void addSystemProperty(String key, String value) {
    _systemProperties.put(key, value);
  }

  public void addJVMProperty(String key, String value) {
    _jvmInformations.put(key, value);
  }

  public void addClassPathProperty(String key, String value) {
    _classPathEntries.put(key, value);
  }

  public String getSparkProperty(String key) {
    return _sparkProperties.getProperty(key);
  }

  public String getSparkProperty(String key, String defaultValue) {
    String val = getSparkProperty(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  public String getSystemProperty(String key) {
    return _systemProperties.getProperty(key);
  }

  public String getJVMInformation(String key) {
    return _jvmInformations.getProperty(key);
  }

  public String getClassPathEntry(String key) {
    return _classPathEntries.getProperty(key);
  }

  public Properties getSparkProperties() {
    return _sparkProperties;
  }

  public Properties getJVMInformations() {
    return _jvmInformations;
  }

  public Properties getClassPathEntries() {
    return _classPathEntries;
  }

  public Properties getSystemProperties() {
    return _systemProperties;
  }

  @Override
  public String toString() {
    return _sparkProperties.toString() + "\n\n\n" + _systemProperties.toString() + "\n\n\n" + _jvmInformations.toString() + "\n\n\n" + _classPathEntries.toString();
  }
}
