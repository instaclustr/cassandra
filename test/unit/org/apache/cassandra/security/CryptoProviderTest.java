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

package org.apache.cassandra.security;

import java.security.Provider;
import java.security.Security;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static com.google.common.collect.ImmutableMap.of;
import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.CRYPTO_PROVIDER_CLASS_NAME;
import static org.apache.cassandra.config.CassandraRelevantProperties.FAIL_ON_MISSING_CRYPTO_PROVIDER;
import static org.apache.cassandra.security.AbstractCryptoProvider.FAIL_ON_MISSING_PROVIDER_KEY;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class CryptoProviderTest
{
    @Test
    public void testCryptoProviderClassSystemProperty()
    {
        try (WithProperties properties = new WithProperties().set(CRYPTO_PROVIDER_CLASS_NAME, TestJREProvider.class.getName()))
        {
            DatabaseDescriptor.daemonInitialization();
            DatabaseDescriptor.applyCryptoProvider();
            assertEquals(TestJREProvider.class.getSimpleName(), DatabaseDescriptor.getCryptoProvider().getProviderName());
        }
    }

    @Test
    public void testFailOnMissingProviderSystemProperty()
    {
        try (WithProperties properties = new WithProperties().set(FAIL_ON_MISSING_CRYPTO_PROVIDER, "true")
                                                             .set(CRYPTO_PROVIDER_CLASS_NAME, InvalidCryptoProvider.class.getName()))
        {
            assertThatExceptionOfType(ConfigurationException.class)
            .isThrownBy(() -> {
                DatabaseDescriptor.daemonInitialization();
                DatabaseDescriptor.applyCryptoProvider();
            })
            .withMessage("The installation of some.package.non.existing.ClassName was not successful, " +
                         "reason: Unable to find crypto provider class 'some.package.non.existing.ClassName'");
        }
    }

    @Test
    public void testCryptoProviderInstallation() throws Exception
    {
        AbstractCryptoProvider provider = new DefaultCryptoProvider(new HashMap<>());
        assertFalse(provider.failOnMissingProvider);

        Provider originalProvider = Security.getProviders()[0];

        provider.install();
        assertTrue(provider.isHealthyInstallation());
        Provider installedProvider = Security.getProviders()[0];
        assertEquals(installedProvider.getName(), provider.getProviderName());

        provider.uninstall();
        Provider currentProvider = Security.getProviders()[0];
        assertNotEquals(currentProvider.getName(), installedProvider.getName());
        assertEquals(originalProvider.getName(), currentProvider.getName());
    }

    @Test
    public void testInvalidProviderInstallator() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        Runnable installator = () ->
        {
            throw new RuntimeException("invalid installator");
        };

        doReturn(installator).when(spiedProvider).installator();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withRootCauseInstanceOf(RuntimeException.class)
        .withMessage("The installation of %s was not successful, reason: invalid installator", spiedProvider.getProviderClassAsString());

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    @Test
    public void testNullInstallatorThrowsException() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        doReturn(null).when(spiedProvider).installator();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withRootCauseInstanceOf(RuntimeException.class)
        .withMessage("The installation of %s was not successful, reason: Installator runnable can not be null!", spiedProvider.getProviderClassAsString());

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    @Test
    public void testProviderHealthcheckerReturningFalse() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        doReturn(false).when(spiedProvider).isHealthyInstallation();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withCause(null)
        .withMessage(format("%s has not passed the health check. " +
                            "Check node's architecture (`uname -m`) is supported, see lib/<arch> subdirectories. " +
                            "The correct architecture-specific library for %s needs to be on the classpath. ",
                            spiedProvider.getProviderName(),
                            spiedProvider.getProviderClassAsString()));

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    @Test
    public void testHealthcheckerThrowingException() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        AbstractCryptoProvider spiedProvider = spy(new DefaultCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true")));

        Throwable t = new RuntimeException("error in health checker");
        doThrow(t).when(spiedProvider).isHealthyInstallation();

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(spiedProvider::install)
        .withCauseInstanceOf(RuntimeException.class)
        .withMessage(format("The installation of %s was not successful, reason: %s",
                            spiedProvider.getProviderClassAsString(), t.getMessage()));

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    @Test
    public void testProviderNotOnClassPathWithPropertyInYaml() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        InvalidCryptoProvider cryptoProvider = new InvalidCryptoProvider(of(FAIL_ON_MISSING_PROVIDER_KEY, "true"));

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(cryptoProvider::install)
        .withMessage("The installation of some.package.non.existing.ClassName was not successful, " +
                     "reason: Unable to find crypto provider class 'some.package.non.existing.ClassName'");

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    @Test
    public void testProviderNotOnClassPathWithSystemProperty() throws Exception
    {
        try (WithProperties properties = new WithProperties().set(FAIL_ON_MISSING_CRYPTO_PROVIDER, "true"))
        {
            String originalProvider = Security.getProviders()[0].getName();

            InvalidCryptoProvider cryptoProvider = new InvalidCryptoProvider(of());

            assertThatExceptionOfType(ConfigurationException.class)
            .isThrownBy(cryptoProvider::install)
            .withMessage("The installation of some.package.non.existing.ClassName was not successful, " +
                         "reason: Unable to find crypto provider class 'some.package.non.existing.ClassName'");

            assertEquals(originalProvider, Security.getProviders()[0].getName());
        }
    }

    @Test
    public void testProviderInstallsJustOnce() throws Exception
    {
        Provider[] originalProviders = Security.getProviders();
        int originalProvidersCount = originalProviders.length;
        Provider originalProvider = Security.getProviders()[0];

        AbstractCryptoProvider provider = new DefaultCryptoProvider(new HashMap<>());
        provider.install();

        assertEquals(provider.getProviderName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount + 1, Security.getProviders().length);

        // install one more time -> it will do nothing

        provider.install();

        assertEquals(provider.getProviderName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount + 1, Security.getProviders().length);

        provider.uninstall();

        assertEquals(originalProvider.getName(), Security.getProviders()[0].getName());
        assertEquals(originalProvidersCount, Security.getProviders().length);
    }

    @Test
    public void testInstallationOfIJREProvider() throws Exception
    {
        String originalProvider = Security.getProviders()[0].getName();

        JREProvider jreProvider = new JREProvider(of());
        jreProvider.install();

        assertEquals(originalProvider, Security.getProviders()[0].getName());
    }

    //  to be public because it is going to be instantiated by reflection in FBUtilties
    public static class TestJREProvider extends JREProvider
    {
        public TestJREProvider(Map<String, String> properties)
        {
            super(properties);
        }

        @Override
        public String getProviderName()
        {
            return TestJREProvider.class.getSimpleName();
        }

        @Override
        public String getProviderClassAsString()
        {
            return TestJREProvider.class.getName();
        }
    }

    private static class InvalidCryptoProvider extends AbstractCryptoProvider
    {
        public InvalidCryptoProvider(Map<String, String> properties)
        {
            super(properties);
        }

        @Override
        public String getProviderName()
        {
            return null;
        }

        @Override
        public String getProviderClassAsString()
        {
            return "some.package.non.existing.ClassName";
        }

        @Override
        protected Runnable installator()
        {
            return () -> {};
        }

        @Override
        protected boolean isHealthyInstallation() throws Exception
        {
            return false;
        }
    }
}
