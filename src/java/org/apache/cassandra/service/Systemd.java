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
package org.apache.cassandra.service;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Library;
import com.sun.jna.Native;
import org.apache.cassandra.utils.FBUtilities;

public class Systemd
{
    private static final Logger logger = LoggerFactory.getLogger(Systemd.class);

    public static final Optional<Api> instance = init();

    private interface RawApi extends Library
    {
        int sd_pid_notify(int pid, int unset, String state);
    }

    public static class Api
    {
        private final RawApi impl;

        private Api(RawApi rawApi)
        {
            this.impl = rawApi;
        }

        public void notifyReady()
        {
            long pid = FBUtilities.getSystemInfo().getPid();
            try
            {
                int returnValue = impl.sd_pid_notify((int) pid, 0, "READY=1");

                if (returnValue <= 0)
                    logger.debug("systemd notify failed for pid {}: {}", pid, returnValue);
                else
                    logger.debug("systemd notified for pid {}, return value: {}", pid, returnValue);
            }
            catch (Throwable t)
            {
                logger.debug("Error occured while invoking sd_pid_notify: {}", t.getMessage());
            }
        }
    }

    private static Optional<Api> init()
    {
        try
        {
            RawApi rawApi = Native.load("systemd", RawApi.class);
            return Optional.of(new Api(rawApi));
        }
        catch (Throwable t)
        {
            logger.debug("systemd support is not available: {}", t.getMessage());
            return Optional.empty();
        }
    }
}
