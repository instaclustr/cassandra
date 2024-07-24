/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils.jmx;

import java.net.InetAddress;
import java.util.Map;
import javax.net.ssl.SSLException;

import org.apache.cassandra.config.EncryptionOptions;

/**
 * The purpose of this interface is to allow customized configuration for the JMX Client and Server Socket factories.
 */
public interface IJmxSocketFactory
{
    /**
     * Configures the client and server socket factories for the JMX connection.
     * Specifically it configures below properties as applicable,
     * <pre>
     *     jmx.remote.rmi.client.socket.factory
     *     jmx.remote.rmi.server.socket.factory
     *     com.sun.jndi.rmi.factory.socket
     * </pre>
     *
     * In case of remote connection enabled, this also sets the following system properties,
     * <pre>
     *     com.sun.management.jmxremote.ssl=true
     *     javax.rmi.ssl.client.enabledCipherSuites=&lt;applicable cipher suites provided in the configuration&gt;
     *     javax.rmi.ssl.client.enabledProtocols=&lt;applicable protocols provided in the configuration&gt;
     * </pre>
     *
     * @param serverAddress the JMX server is bound to
     * @param localOnly {@code true} if the JMX server only allows local connections; {@code false} if the JMX server
     *                              allows the remote connections.
     * @param jmxEncryptionOptions {@link EncryptionOptions} used for the SSL configuration in case of the remote
     *                                                      connections. Could be {@code null} if system properties are
     *                                                      used instead as per <a href="https://docs.oracle.com/en/java/javase/17/management/monitoring-and-management-using-jmx-technology.html#GUID-F08985BB-629A-4FBF-A0CB-8762DF7590E0">Java Documentation</a>
     * @return Map&lt;String, Object@gt; containing {@code jmx.remote.rmi.client.socket.factory}, {@code jmx.remote.rmi.server.socket.factory}
     * and {@code com.sun.jndi.rmi.factory.socket} properties for the client and server socket factories.
     * @throws SSLException if it fails to configure the socket factories with the given input
     */
    Map<String, Object> configure(InetAddress serverAddress, boolean localOnly, EncryptionOptions jmxEncryptionOptions)
    throws SSLException;
}
