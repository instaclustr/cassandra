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

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.locator.AbstractCloudMetadataServiceConnector.DefaultCloudMetadataServiceConnector;

/**
 * Generic cloud-based metadata service snitch which parses datacenter and rack of a node by
 * executing GET query for URL configured in cassandra-rackdc.properties under
 * key {@link AbstractCloudMetadataServiceConnector#METADATA_URL_PROPERTY}. It expects HTTP response code 200.
 * It is not passing any headers to GET request.
 * <p>
 * The response will be parsed like following - if the response body is, for example "us-central1-a", then
 * the datacenter will be "us-central1" and rack will be "a". There is value of "dc_suffix" key in
 * cassandra-rackdc.properties appended to datacenter.
 */
public class GenericCloudMetadataServiceSnitch extends AbstractCloudMetadataServiceSnitch
{
    public GenericCloudMetadataServiceSnitch() throws IOException
    {
        this(new SnitchProperties());
    }

    public GenericCloudMetadataServiceSnitch(SnitchProperties properties) throws IOException
    {
        this(properties, new DefaultCloudMetadataServiceConnector(properties));
    }

    @VisibleForTesting
    public GenericCloudMetadataServiceSnitch(SnitchProperties snitchProperties, AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector, snitchProperties, SnitchUtils.parseDcAndRack(connector.apiCall("", ImmutableMap.of()), snitchProperties.getDcSuffix()));
    }
}
