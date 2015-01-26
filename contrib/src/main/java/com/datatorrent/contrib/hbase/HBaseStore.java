/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.lib.db.Connectable;
/**
 * A {@link Connectable} that uses HBase to connect to stores and implements Connectable interface. 
 * <p>
 * @displayName HBase Store
 * @category Store
 * @tags store
 * @since 1.0.2
 */
public class HBaseStore implements Connectable {

  public static final String USER_NAME_SPECIFIER = "%USER_NAME%";
  
  private static final Logger logger = LoggerFactory.getLogger(HBaseStore.class);
  
  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;
  
  protected String principal;
  protected String keytabPath;
  // Default interval 30 min
  protected long renewCheckInterval = 30 * 60 * 1000;
  protected transient Thread renewer;
  private volatile transient boolean doRenew;

  protected transient HTable table;

  /**
   * Get the zookeeper quorum location.
   * 
   * @return The zookeeper quorum location
   */
  public String getZookeeperQuorum() {
    return zookeeperQuorum;
  }

  /**
   * Set the zookeeper quorum location.
   * 
   * @param zookeeperQuorum
   *            The zookeeper quorum location
   */
  public void setZookeeperQuorum(String zookeeperQuorum) {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  /**
   * Get the zookeeper client port.
   * 
   * @return The zookeeper client port
   */
  public int getZookeeperClientPort() {
    return zookeeperClientPort;
  }

  /**
   * Set the zookeeper client port.
   * 
   * @param zookeeperClientPort
   *            The zookeeper client port
   */
  public void setZookeeperClientPort(int zookeeperClientPort) {
    this.zookeeperClientPort = zookeeperClientPort;
  }

  /**
   * Get the HBase table name.
   * 
   * @return The HBase table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Set the HBase table name.
   * 
   * @param tableName
   *            The HBase table name
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Get the Kerberos principal.
   *
   * @return The Kerberos principal
   */
  public String getPrincipal()
  {
    return principal;
  }

  /**
   * Set the Kerberos principal.
   *
   * @param principal
   *            The Kerberos principal
   */
  public void setPrincipal(String principal)
  {
    this.principal = principal;
  }

  /**
   * Get the Kerberos keytab path
   *
   * @return The Kerberos keytab path
   */
  public String getKeytabPath()
  {
    return keytabPath;
  }

  /**
   * Set the Kerberos keytab path.
   *
   * @param keytabPath
   *            The Kerberos keytab path
   */
  public void setKeytabPath(String keytabPath)
  {
    this.keytabPath = keytabPath;
  }

  public long getRenewCheckInterval()
  {
    return renewCheckInterval;
  }

  public void setRenewCheckInterval(long renewCheckInterval)
  {
    this.renewCheckInterval = renewCheckInterval;
  }

  /**
   * Get the HBase table .
   * 
   * @return The HBase table
   */
  public HTable getTable() {
    return table;
  }

  /**
   * Set the HBase table.
   * 
   * @param table
   *            The HBase table
   */
  public void setTable(HTable table) {
    this.table = table;
  }

  /**
   * Get the configuration.
   * 
   * @return The configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Set the configuration.
   * 
   * @param configuration
   *            The configuration
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  protected transient Configuration configuration;

  @Override
  public void connect() throws IOException {
    if ((principal != null) && (keytabPath != null)) {
      String lprincipal = evaluateProperty(principal);
      String lkeytabPath = evaluateProperty(keytabPath);
      UserGroupInformation.loginUserFromKeytab(lprincipal, lkeytabPath);
      doRenew = true;
      renewer = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            while (doRenew) {
              Thread.sleep(renewCheckInterval);
              try {
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
              } catch (IOException e) {
                logger.error("Error trying to relogin from keytab", e);
              }
            }
          } catch (InterruptedException e) {
            if (doRenew) {
              logger.warn("Renewer interrupted... stopping");
            }
          }
        }
      });
      renewer.start();
    }
    configuration = HBaseConfiguration.create();
    // The default configuration is loaded from resources in classpath, the following parameters can be optionally set
    // to override defaults
    if (zookeeperQuorum != null) {
      configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    }
    if (zookeeperClientPort != 0) {
      configuration.set("hbase.zookeeper.property.clientPort", "" + zookeeperClientPort);
    }
    table = new HTable(configuration, tableName);
    table.setAutoFlushTo(false);

  }
  
  private String evaluateProperty(String property) throws IOException
  {
    if (property.contains(USER_NAME_SPECIFIER)) {
     property = property.replaceAll(USER_NAME_SPECIFIER, UserGroupInformation.getLoginUser().getShortUserName()); 
    }
    return property;
  }

  @Override
  public void disconnect() throws IOException {
    if (renewer != null) {
      doRenew = false;
      renewer.interrupt();
      try {
        renewer.join();
      } catch (InterruptedException e) {
        logger.warn("Unsuccessful waiting for renewer to finish. Proceeding to shutdown", e);
      }
    }
  }

  @Override
  public boolean isConnected() {
    // not applicable to hbase
    return false;
  }

}
