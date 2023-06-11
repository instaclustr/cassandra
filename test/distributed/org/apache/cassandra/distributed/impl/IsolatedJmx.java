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

package org.apache.cassandra.distributed.impl;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Predicate;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.management.remote.rmi.RMIJRMPServerImpl;

import org.slf4j.Logger;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.RMIClientSocketFactoryImpl;
import org.apache.cassandra.utils.ReflectionUtils;
import sun.rmi.transport.tcp.TCPEndpoint;

import static org.apache.cassandra.distributed.api.Feature.JMX;
import static org.apache.cassandra.utils.MBeanWrapper.IS_DISABLED_MBEAN_REGISTRATION;

public class IsolatedJmx
{
    /** Controls the JMX server threadpool keap-alive time. */
    private static final String SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME = "sun.rmi.transport.tcp.threadKeepAliveTime";
    /** Controls the distributed garbage collector lease time for JMX objects. */
    private static final String JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST ="java.rmi.dgc.leaseValue";
    private static final int RMI_KEEPALIVE_TIME = 1000;

    private JMXConnectorServer jmxConnectorServer;
    private JMXServerUtils.JmxRegistry registry;
    private RMIJRMPServerImpl jmxRmiServer;
    private MBeanWrapper.InstanceMBeanWrapper wrapper;
    private RMIClientSocketFactoryImpl clientSocketFactory;
    private CollectingRMIServerSocketFactoryImpl serverSocketFactory;
    private Logger inInstancelogger;
    private IInstanceConfig config;

    public IsolatedJmx(Instance instance, Logger inInstancelogger) {
        this.inInstancelogger = inInstancelogger;
        config = instance.config();
    }

    public void startJmx()
    {
        try
        {
            // Several RMI threads hold references to in-jvm dtest objects, and are, by default, kept
            // alive for long enough (minutes) to keep classloaders from being collected.
            // Set these two system properties to a low value to allow cleanup to occur fast enough
            // for GC to collect our classloaders.
            System.setProperty(JAVA_RMI_DGC_LEASE_VALUE_IN_JVM_DTEST, String.valueOf(RMI_KEEPALIVE_TIME));
            System.setProperty(SUN_RMI_TRANSPORT_TCP_THREADKEEPALIVETIME, String.valueOf(RMI_KEEPALIVE_TIME));
            System.setProperty(IS_DISABLED_MBEAN_REGISTRATION, "false");
            InetAddress addr = config.broadcastAddress().getAddress();

            int jmxPort = config.jmxPort();

            String hostname = addr.getHostAddress();
            wrapper = new MBeanWrapper.InstanceMBeanWrapper(hostname + ":" + jmxPort);
            ((MBeanWrapper.DelegatingMbeanWrapper) MBeanWrapper.instance).setDelegate(wrapper);
            Map<String, Object> env = new HashMap<>();

            serverSocketFactory = new CollectingRMIServerSocketFactoryImpl(addr);
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                    serverSocketFactory);
            clientSocketFactory = new RMIClientSocketFactoryImpl(addr);
            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE,
                    clientSocketFactory);

            // configure the RMI registry
            registry = new JMXServerUtils.JmxRegistry(jmxPort,
                                                      clientSocketFactory,
                                                      serverSocketFactory,
                                                      "jmxrmi");

            // Mark the JMX server as a permanently exported object. This allows the JVM to exit with the
            // server running and also exempts it from the distributed GC scheduler which otherwise would
            // potentially attempt a full GC every `sun.rmi.dgc.server.gcInterval` millis (default is 3600000ms)
            // For more background see:
            //   - CASSANDRA-2967
            //   - https://www.jclarity.com/2015/01/27/rmi-system-gc-unplugged/
            //   - https://bugs.openjdk.java.net/browse/JDK-6760712
            env.put("jmx.remote.x.daemon", "true");

            // Set the port used to create subsequent connections to exported objects over RMI. This simplifies
            // configuration in firewalled environments, but it can't be used in conjuction with SSL sockets.
            // See: CASSANDRA-7087
            int rmiPort = config.jmxPort();

            // We create the underlying RMIJRMPServerImpl so that we can manually bind it to the registry,
            // rather then specifying a binding address in the JMXServiceURL and letting it be done automatically
            // when the server is started. The reason for this is that if the registry is configured with SSL
            // sockets, the JMXConnectorServer acts as its client during the binding which means it needs to
            // have a truststore configured which contains the registry's certificate. Manually binding removes
            // this problem.
            // See CASSANDRA-12109.
            jmxRmiServer = new RMIJRMPServerImpl(rmiPort, clientSocketFactory, serverSocketFactory,
                                                 env);
            JMXServiceURL serviceURL = new JMXServiceURL("rmi", hostname, rmiPort);
            jmxConnectorServer = new RMIConnectorServer(serviceURL, env, jmxRmiServer, wrapper.getMBeanServer());

            jmxConnectorServer.start();

            registry.setRemoteServerStub(jmxRmiServer.toStub());
            JMXServerUtils.logJmxServiceUrl(addr, jmxPort);
            waitForJmxAvailability(hostname, jmxPort, env);
        }
        catch (Throwable e)
        {
            throw new RuntimeException("Feature.JMX was enabled but could not be started.", e);
        }
    }


    private void waitForJmxAvailability(String hostname, int rmiPort, Map<String, Object> env) throws InterruptedException, MalformedURLException
    {
        String url = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", hostname, rmiPort);
        JMXServiceURL serviceURL = new JMXServiceURL(url);
        int attempts = 0;
        Throwable lastThrown = null;
        while (attempts < 20)
        {
            attempts++;
            try (JMXConnector ignored = JMXConnectorFactory.connect(serviceURL, env))
            {
                inInstancelogger.info("Connected to JMX server at {} after {} attempt(s)",
                                      url, attempts);
                return;
            }
            catch (MalformedURLException e)
            {
                throw new RuntimeException(e);
            }
            catch (Throwable thrown)
            {
                lastThrown = thrown;
            }
            inInstancelogger.info("Could not connect to JMX on {} after {} attempts. Will retry.", url, attempts);
            Thread.sleep(1000);
        }
        throw new RuntimeException("Could not start JMX - unreachable after 20 attempts", lastThrown);
    }

    public void stopJmx() throws IllegalAccessException, NoSuchFieldException, InterruptedException
    {
        if (!config.has(JMX))
            return;
        // First, swap the mbean wrapper back to a NoOp wrapper
        // This prevents later attempts to unregister mbeans from failing in Cassandra code, as we're going to
        // unregister all of them here
        ((MBeanWrapper.DelegatingMbeanWrapper) MBeanWrapper.instance).setDelegate(new MBeanWrapper.NoOpMBeanWrapper());
        try
        {
            wrapper.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close wrapper.", e);
        }
        try
        {
            jmxConnectorServer.stop();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close jmxConnectorServer.", e);
        }
        try
        {
            registry.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close registry.", e);
        }
        try
        {
            serverSocketFactory.close();
        }
        catch (Throwable e)
        {
            inInstancelogger.warn("failed to close serverSocketFactory.", e);
        }
        // The TCPEndpoint class holds references to a class in the in-jvm dtest framework
        // which transitively has a reference to the InstanceClassLoader, so we need to
        // make sure to remove the reference to them when the instance is shutting down.
        // Additionally, we must make sure to only clear endpoints created by this instance
        // As clearning the entire map can cause issues with starting and stopping nodes mid-test.
        clearMapField(TCPEndpoint.class, null, "localEndpoints", this::endpointCreateByThisInstance);
        Thread.sleep(2 * RMI_KEEPALIVE_TIME); // Double the keep-alive time to give Distributed GC some time to clean up
    }

    private boolean endpointCreateByThisInstance(Map.Entry<Object, LinkedList<TCPEndpoint>> entry)
    {
        return entry.getValue().stream().anyMatch(ep -> ep.getServerSocketFactory() == this.serverSocketFactory && ep.getClientSocketFactory() == this.clientSocketFactory);
    }

    private <K, V> void clearMapField(Class<?> clazz, Object instance, String mapName, Predicate<Map.Entry<K, V>> shouldRemove)
    throws IllegalAccessException, NoSuchFieldException {
        Field mapField = ReflectionUtils.getField(clazz, mapName);
        mapField.setAccessible(true);
        Map<K, V> map = (Map<K, V>) mapField.get(instance);
        // Because multiple instances can be shutting down at once,
        // synchronize on the map to avoid ConcurrentModificationException
        synchronized (map)
        {
            for (Iterator<Map.Entry<K, V>> it = map.entrySet().iterator(); it.hasNext(); )
            {
                Map.Entry<K, V> entry = it.next();
                if (shouldRemove.test(entry))
                {
                    it.remove();
                }
            }
        }
    }
}
