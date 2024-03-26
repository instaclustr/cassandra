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

package org.apache.cassandra.locator;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.apache.cassandra.locator.HttpServiceConnector.METADATA_REQUEST_HEADERS_PROPERTY;
import static org.apache.cassandra.locator.HttpServiceConnector.METADATA_URL_PROPERTY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpSeedProviderTest
{
    @Rule
    public final WireMockRule service = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8080));

    @Rule
    public final WireMockRule service2 = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8081));

    @Rule
    public final WireMockRule service3 = new WireMockRule(wireMockConfig().bindAddress("127.0.0.1").port(8082));

    @Test
    public void testProvider()
    {
        String response = "127.0.0.2\n127.0.0.3  \n127.0.0.3\n127.0.0.4\n \n";

        service.stubFor(get(urlEqualTo("/seeds"))
                        .willReturn(aResponse().withBody(response)
                                               .withStatus(200)));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds");
        assertEquals(3, new HttpSeedProvider(p).getSeeds().size());

        assertEquals(3, new HttpSeedProvider(new HashMap<>()
        {{
            put(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds");
        }}).getSeeds().size());
    }

    @Test
    public void testProviderWithSeedsWithPorts() throws Throwable
    {
        String response = "127.0.0.2:8999\n127.0.0.3  \n127.0.0.3\n127.0.0.4:1234\n \n";

        service.stubFor(get(urlEqualTo("/seeds"))
                        .willReturn(aResponse().withBody(response)
                                               .withStatus(200)));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds");

        List<InetAddressAndPort> seeds = new HttpSeedProvider(p).getSeeds();
        assertEquals(3, seeds.size());

        assertEquals(InetAddressAndPort.getByName("127.0.0.2:8999"), seeds.get(0));
        assertEquals(InetAddressAndPort.getByName("127.0.0.3:7000"), seeds.get(1));
        assertEquals(InetAddressAndPort.getByName("127.0.0.4:1234"), seeds.get(2));
    }

    @Test
    public void testProviderWithHeaders()
    {
        String response = "127.0.0.2\n127.0.0.3  \n127.0.0.3\n127.0.0.4\n \n";

        service.stubFor(get(urlEqualTo("/seeds"))
                        .withHeader("myheader", new EqualToPattern("myheadervalue"))
                        .withHeader("anotherheader", new EqualToPattern("anothervalue"))
                        .willReturn(aResponse().withBody(response)
                                               .withStatus(200)));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds");
        p.setProperty(METADATA_REQUEST_HEADERS_PROPERTY, "myheader=myheadervalue,anotherheader=anothervalue");

        assertEquals(3, new HttpSeedProvider(p).getSeeds().size());
    }

    @Test
    public void testInvalidSeed()
    {
        String response = "thisissomenonexistingurl";

        service.stubFor(get(urlEqualTo("/seeds"))
                        .willReturn(aResponse().withBody(response)
                                               .withStatus(200)));

        assertEquals(0, new HttpSeedProvider(new HashMap<>()
        {{
            put(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds");
        }}).getSeeds().size());
    }

    @Test
    public void testMultipleUrls() throws Exception
    {
        String response = "127.0.0.2\n127.0.0.3  \n127.0.0.3\n127.0.0.4\n \n";
        String response2 = "127.0.0.5\n127.0.0.6\nthisissomenonexistingurl";

        service.stubFor(get(urlEqualTo("/seeds"))
                        .willReturn(aResponse().withBody(response)
                                               .withStatus(404)));

        service2.stubFor(get(urlEqualTo("/seeds2"))
                         .willReturn(aResponse().withBody(response2)
                                                .withStatus(200)));

        service3.stubFor(get(urlEqualTo("/seeds3"))
                         .willReturn(aResponse().withBody(response)
                                                .withStatus(200)));

        Properties p = new Properties();
        p.setProperty(METADATA_URL_PROPERTY, "http://127.0.0.1:8080/seeds,http://127.0.0.1:8083/seeds,http://127.0.0.1:8081/seeds2,http://127.0.0.1:8082/seeds3");

        List<InetAddressAndPort> seeds = new HttpSeedProvider(p).getSeeds();

        assertEquals(2, seeds.size());

        assertTrue(seeds.contains(InetAddressAndPort.getByName("127.0.0.5")));
        assertTrue(seeds.contains(InetAddressAndPort.getByName("127.0.0.6")));
    }
}
