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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class HttpSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(HttpSeedProvider.class);
    private static final Pattern NEW_LINES_PATTERN = Pattern.compile("\\n");

    private final HttpServiceConnector serviceConnector;

    public HttpSeedProvider(Map<String, String> parameters)
    {
        this(getProperties(parameters));
    }

    HttpSeedProvider(Properties properties)
    {
        serviceConnector = new HttpServiceConnector(properties);
    }

    @Override
    public List<InetAddressAndPort> getSeeds()
    {
        try
        {
            String response = serviceConnector.apiCall("");

            List<String> lines = Arrays.stream(NEW_LINES_PATTERN.split(response))
                                       .map(String::trim)
                                       .filter(s -> !s.isBlank())
                                       .distinct()
                                       .collect(toList());

            List<InetAddressAndPort> seeds = new ArrayList<>(lines.size());

            for (String line : lines)
            {
                try
                {
                    seeds.add(InetAddressAndPort.getByName(line));
                }
                catch (UnknownHostException ex)
                {
                    logger.warn("Seed provider couldn't lookup host {}", line);
                }
            }

            return seeds;
        }
        catch (Exception e)
        {
            logger.error("Exception occured while resolving seeds, returning empty seeds list.", e);
            return Collections.emptyList();
        }
    }

    private static Properties getProperties(Map<String, String> args)
    {
        if (args == null)
            return new Properties();

        Properties properties = new Properties();
        properties.putAll(args);
        return properties;
    }
}
