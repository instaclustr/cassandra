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

package org.apache.cassandra.distributed.test;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.HttpSeedProvider;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.cassandra.distributed.Constants.KEY_DTEST_FULL_STARTUP;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.locator.HttpServiceConnector.METADATA_URL_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class HttpSeedProviderTest extends TestBaseImpl
{
    @Rule
    public final WireMockRule service = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8085));

    @Rule
    public final WireMockRule service2 = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8086));

    @Test
    public void testHttpSeedProvider() throws Exception
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                                      .withConfig(config -> config.with(NETWORK, GOSSIP))
                                      .start())
        {
            service.stubFor(get(urlEqualTo("/seeds"))
                            .willReturn(aResponse().withBody("127.0.0.1:7012")
                                                   .withStatus(200)));

            service2.stubFor(get(urlEqualTo("/seeds2"))
                             .willReturn(aResponse().withStatus(404)));

            IInstanceConfig config = cluster.newInstanceConfig()
                                            .set("auto_bootstrap", true)
                                            .set("seed_provider", new HashMap<>()
                                            {{
                                                put("class_name", HttpSeedProvider.class.getName());
                                                put("parameters", new HashMap<>()
                                                {{
                                                    put(METADATA_URL_PROPERTY, "http://127.0.0.1:8086/seeds2,http://127.0.0.1:8085/seeds");
                                                }});
                                            }})
                                            .set(KEY_DTEST_FULL_STARTUP, true);

            final AtomicInteger service1ContactCounter = new AtomicInteger();
            final AtomicInteger service2ContactCounter = new AtomicInteger();

            service.addMockServiceRequestListener((request, response) -> service1ContactCounter.incrementAndGet());
            service2.addMockServiceRequestListener((request, response) -> service2ContactCounter.incrementAndGet());

            IInvokableInstance newInstance = cluster.bootstrap(config);
            newInstance.startup();

            assertThat(service1ContactCounter.get()).isPositive();
            assertThat(service2ContactCounter.get()).isPositive();

            String seedProvider = (String) cluster.get(2)
                                                  .coordinator()
                                                  .execute("select value from system_views.settings where name = 'seed_provider.class_name'", ONE)[0][0];

            assertEquals(HttpSeedProvider.class.getName(), seedProvider);

            String parameters = (String) cluster.get(2)
                                                .coordinator()
                                                .execute("select value from system_views.settings where name = 'seed_provider.parameters'", ONE)[0][0];

            assertThat(parameters).contains("{metadata_url=http://127.0.0.1:8086/seeds2,http://127.0.0.1:8085/seeds}");
        }
    }
}
