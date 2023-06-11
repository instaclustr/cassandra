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

package org.apache.cassandra.distributed.test.jmx;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.JMXUtil;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class JMXFeatureTest extends TestBaseImpl
{
    /**
     * Test the in-jvm dtest JMX feature.
     * - Create a cluster with multiple JMX servers, one per instance
     * - Test that when connecting, we get the correct MBeanServer by checking the default domain, which is set to the IP of the instance
     * - Run the test multiple times to ensure cleanup of the JMX servers is complete so the next test can run successfully using the same host/port.
     * NOTE: In later versions of Cassandra, there is also a `testOneNetworkInterfaceProvisioning` that leverages the ability to specify
     * ports in addition to IP/Host for binding, but this version does not support that feature. Keeping the test name the same
     * so that it's consistent across versions.
     *
     * @throws Exception
     */
    @Test
    public void testMultipleNetworkInterfacesProvisioning() throws Exception
    {
        int iterations = 2; // Make sure the JMX infrastructure all cleans up properly by running this multiple times.
        Set<String> allInstances = new HashSet<>();
        for (int i = 0; i < iterations; i++)
        {
            try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(Feature.values())).start())
            {
                Set<String> instancesContacted = new HashSet<>();
                for (IInvokableInstance instance : cluster.get(1, 2))
                {
                    testInstance(instancesContacted, instance);
                }
                Assert.assertEquals("Should have connected with both JMX instances.", 2, instancesContacted.size());
                allInstances.addAll(instancesContacted);
            }
        }
        Assert.assertEquals("Each instance from each cluster should have been unique", iterations * 2, allInstances.size());
    }

    @Test
    public void testShutDownAndRestartInstances() throws Throwable
    {
        try (Cluster cluster = Cluster.build(2).withConfig(c -> c.with(Feature.values())).start())
        {
            IInvokableInstance instanceToStop = cluster.get(1);
            Futures.getUnchecked(instanceToStop.shutdown());
            // NOTE: This would previously fail because we cleared everything from the TCPEndpoint map in IsolatedJmx.
            // Now, we only clear the endpoints related to that instance, which prevents this code from
            // breaking with a `java.net.BindException: Address already in use (Bind failed)`
            NodeToolResult statusResult = cluster.get(2).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            waitUntil(() -> cluster.get(2).nodetoolResult("status").getStdout(),
                      (i) -> i.contains("DN  127.0.0.1"));

            instanceToStop.startup();
            statusResult = cluster.get(1).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            waitUntil(statusResult::getStdout,
                      (i) -> i.contains("UN  127.0.0.1"));

            statusResult = cluster.get(2).nodetoolResult("status");
            Assert.assertEquals(0, statusResult.getRc());
            Assert.assertThat(statusResult.getStderr(), is(blankOrNullString()));
            waitUntil(() -> cluster.get(2).nodetoolResult("status").getStdout(),
                      (i) -> i.contains("UN  127.0.0.1"));

        }
    }

    public void waitUntil(Supplier<String> inputSupplier, Predicate<String> test) throws Throwable
    {
        for (int i = 0; i < 60; i++)
        {
            if (test.test(inputSupplier.get()))
                return;

            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        Assert.fail();
    }

    private void testInstance(Set<String> instancesContacted, IInvokableInstance instance) throws IOException
    {
        IInstanceConfig config = instance.config();
        try (JMXConnector jmxc = JMXUtil.getJmxConnector(config))
        {
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            // instances get their default domain set to their IP address, so us it
            // to check that we are actually connecting to the correct instance
            String defaultDomain = mbsc.getDefaultDomain();
            instancesContacted.add(defaultDomain);
            Assert.assertThat(defaultDomain, startsWith(JMXUtil.getJmxHost(config) + ":" + config.jmxPort()));
        }
    }
}
