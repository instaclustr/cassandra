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

package org.apache.cassandra.service.storage;

import java.net.URL;
import java.util.Arrays;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;


public class StorageProviders
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProviders.class);

    private static final ChannelProxyFactory LOCAL = ChannelProxyFactory.LOCAL;
    private static volatile ChannelProxyFactory PLUGIN = null;

    public static void initialize()
    {
        StorageProviderConfig cfg = DatabaseDescriptor.getStorageProviderConfig();
        if (cfg == null) return;

        File dir = new File(cfg.pluginDir); // e.g. lib/providers/s3
        URL[] jars = Arrays.stream(dir.tryList(f -> f.name().endsWith(".jar")))
                           .map(f -> {
                               try
                               {
                                   return f.toJavaIOFile().toURL();
                               }
                               catch (Throwable t)
                               {
                                   throw new RuntimeException(String.format("Invalid file %s to create URL for.",
                                                                            f.toString()));
                               }
                           })
                           .toArray(URL[]::new);

        ClassLoader pluginLoader = new PluginClassLoader(jars, StorageProviders.class.getClassLoader());

        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader(pluginLoader);
            PLUGIN = ServiceLoader.load(ChannelProxyFactory.class, pluginLoader)
                                  .findFirst()
                                  .orElseThrow(() -> new ConfigurationException("No ChannelProxyFactory found in " + dir));

            PLUGIN.configure(cfg.options);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(saved);
        }

        logger.info("Storage provider {} loaded from {}", LOCAL.getClass().getName(), dir);
    }

    public static ChannelProxyFactory factory()
    {
        return PLUGIN == null ? ChannelProxyFactory.LOCAL : PLUGIN;
    }
}
