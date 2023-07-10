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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.FAIL_ON_MISSING_CRYPTO_PROVIDER;

public abstract class AbstractCryptoProvider
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractCryptoProvider.class);
    public static final String FAIL_ON_MISSING_PROVIDER_KEY = "fail_on_missing_provider";

    protected final boolean failOnMissingProvider;

    public AbstractCryptoProvider(Map<String, String> properties)
    {
        boolean failOnMissingProviderFromProperties = properties != null && Boolean.parseBoolean(properties.getOrDefault(FAIL_ON_MISSING_PROVIDER_KEY, "false"));
        failOnMissingProvider = FAIL_ON_MISSING_CRYPTO_PROVIDER.getBoolean(failOnMissingProviderFromProperties);
    }

    /**
     * Returns name of the provider, as returned from {@link Provider#getName()}
     *
     * @return name of the provider
     */
    public abstract String getProviderName();

    /**
     * Returns the name of the class which installs specific provider of name {@link #getProviderName()}.
     *
     * @return name of class of provider
     */
    public abstract String getProviderClassAsString();

    /**
     * Returns a runnable which installs this crypto provider.
     *
     * @return runnable which installs this provider
     */
    protected abstract Runnable installator();

    /**
     * Returns boolean telling if this provider was installed properly.
     *
     * @return {@code true} if provider was installed properly, {@code false} otherwise.
     */
    protected abstract boolean isHealthyInstallation() throws Exception;

    /**
     * The default installation runs {@link AbstractCryptoProvider#installator()} and after that
     * {@link AbstractCryptoProvider#isHealthyInstallation()}.
     * <p>
     * If any step fails, it will not throw an exception unless the parameter
     * {@link AbstractCryptoProvider#FAIL_ON_MISSING_PROVIDER_KEY} is {@code true}.
     */
    public void install() throws Exception
    {
        if (JREProvider.class.getName().equals(getProviderClassAsString()))
        {
            logger.debug(format("Installation of a crypto provider was skipped as %s was used.", JREProvider.class.getName()));
            return;
        }

        String failureMessage = null;
        Throwable t = null;
        try
        {
            FBUtilities.classForName(getProviderClassAsString(), "crypto provider");

            Provider currentHighestPriorityProvider = getHighestPriorityProvider();
            if (currentHighestPriorityProvider != null && currentHighestPriorityProvider.getName().equals(getProviderName()))
            {
                logger.debug("{} was already installed", getProviderName());
                return;
            }
            else
            {
                Runnable r = installator();
                if (r == null)
                    throw new IllegalStateException("Installator runnable can not be null!");
                else
                    r.run();
            }

            if (isHealthyInstallation())
                logger.info("{} health check OK", getProviderName());
            else
                failureMessage = format("%s has not passed the health check. " +
                                        "Check node's architecture (`uname -m`) is supported, see lib/<arch> subdirectories. " +
                                        "The correct architecture-specific library for %s needs to be on the classpath. ",
                                        getProviderName(),
                                        getProviderClassAsString());
        }
        catch (ClassNotFoundException ex)
        {
            failureMessage = getProviderClassAsString() + " is not on the class path!";
        }
        catch (Exception ex)
        {
            failureMessage = format("The installation of %s was not successful, reason: %s",
                                    getProviderClassAsString(), ex.getMessage());
            t = ex;
        }

        if (failureMessage != null)
        {
            // To be sure there is not any leftover, proactively remove this provider in case of any failure.
            // This method returns silently if the provider is not installed or if name is null.
            try
            {
                uninstall();
            }
            catch (Throwable throwable)
            {
                logger.warn("Uninstallation of {} failed", getProviderName(), throwable);
            }

            if (failOnMissingProvider)
                throw new ConfigurationException(failureMessage, t);
            else
                logger.warn(failureMessage);
        }
    }

    /**
     * Uninstalls this crypto provider of name {@link #getProviderName()}
     *
     * @see Security#removeProvider(String)
     */
    public void uninstall()
    {
        Security.removeProvider(getProviderName());
    }

    private Provider getHighestPriorityProvider()
    {
        Provider[] providers = Security.getProviders();
        return providers.length > 0 ? providers[0] : null;
    }
}
