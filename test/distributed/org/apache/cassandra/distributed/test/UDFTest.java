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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ICoordinator;

import static com.google.common.collect.Maps.difference;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.Cluster.create;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertTrue;

public class UDFTest extends TestBaseImpl
{
    private static final String INSERT_TEMPLATE = "INSERT INTO %s.current (city, timestamp, location, measurement)\n" +
                                                  "VALUES ('%s', '%s', {coordinate: {latitude: %f, longitude: %f}, elevation: %f}, %f)";

    private static final String SELECT_TEMPLATE = "SELECT %s.city_measurements(city, measurement, 16.5) AS m FROM %s.current";

    private static final String[] SCHEMA_STATEMETS = {
    "CREATE TYPE " + KEYSPACE + ".coordinate (latitude decimal, longitude decimal)",
    "CREATE TYPE " + KEYSPACE + ".location (coordinate FROZEN<coordinate>, elevation double)",
    "CREATE TABLE " + KEYSPACE + ".current (city text, timestamp timestamp, location location, measurement double, t_uuid timeuuid, PRIMARY KEY (city, timestamp))",
    "CREATE TYPE IF NOT EXISTS " + KEYSPACE + ".aggst (lt int, ge int)",
    "CREATE FUNCTION " + KEYSPACE + ".city_measurements_sfunc (state map<text, frozen<aggst>>, city text, measurement double, threshold double)\n" +
    "RETURNS NULL ON NULL INPUT\n" +
    "RETURNS map<text, frozen<aggst>> LANGUAGE java AS $$\n" +
    "UDTValue a = (UDTValue)state.get(city);\n" +
    "if (a == null) {\n" +
    "    a = udfContext.newUDTValue(\"aggst\");\n" +
    "    if (measurement < threshold) {\n" +
    "        a.setInt(\"lt\", 1);\n" +
    "        a.setInt(\"ge\", 0);\n" +
    "    }\n" +
    "    else {\n" +
    "        a.setInt(\"lt\", 0);\n" +
    "        a.setInt(\"ge\", 1);\n" +
    "    }\n" +
    "}\n" +
    "else {\n" +
    "    if (measurement < threshold) {\n" +
    "        a.setInt(\"lt\", a.getInt(\"lt\") + 1);\n" +
    "    }\n" +
    "    else {\n" +
    "        a.setInt(\"ge\", a.getInt(\"ge\") + 1);\n" +
    "    }\n" +
    "}\n" +
    "state.put(city, a);\n" +
    "return state;\n" +
    "$$;",
    "CREATE AGGREGATE " + KEYSPACE + ".city_measurements (text, double, double)\n" +
    "SFUNC city_measurements_sfunc\n" +
    "STYPE map<text, frozen<aggst>>\n" +
    "INITCOND {};\n"
    };

    private static final LinkedHashMap<String, Map<String, Integer>> EXPECTED_RESULT = new LinkedHashMap<>()
    {{
        put("Boston", new LinkedHashMap<>()
        {{
            put("lt", 0);
            put("ge", 24);
        }});

        put("Helsinki", new LinkedHashMap<>()
        {{
            put("lt", 8);
            put("ge", 16);
        }});
    }};

    private ICoordinator coordinator;

    @Test
    public void testReloadAfterStop() throws Exception
    {
        try (Cluster cluster = init(create(1, config -> config.set("enable_user_defined_functions", "true"))))
        {
            coordinator = cluster.coordinator(1);

            populateDatabase(cluster);

            assertTrue(difference(EXPECTED_RESULT, performSelectQuery()).areEqual());

            restart(cluster);

            assertTrue(difference(EXPECTED_RESULT, performSelectQuery()).areEqual());
        }
    }

    private Map<String, Map<String, Integer>> performSelectQuery()
    {
        Object[][] queryResults = coordinator.execute(format(SELECT_TEMPLATE, KEYSPACE, KEYSPACE), ALL);

        LinkedHashMap<String, Map<String, Integer>> results = new LinkedHashMap<>();

        List<FieldIdentifier> names = new ArrayList<>();
        names.add(FieldIdentifier.forQuoted("lt"));
        names.add(FieldIdentifier.forQuoted("ge"));
        List<AbstractType<?>> types = new ArrayList<>();
        types.add(Int32Type.instance);
        types.add(Int32Type.instance);
        UserType aggst = new UserType(KEYSPACE, UTF8Type.instance.fromString("aggst"), names, types, true);

        for (Object[] result : queryResults)
        {
            for (Map.Entry<String, ByteBuffer> entry : ((LinkedHashMap<String, ByteBuffer>) result[0]).entrySet())
            {
                ByteBuffer[] split = aggst.split(ByteBufferAccessor.instance, entry.getValue());
                results.put(entry.getKey(), new LinkedHashMap<String, Integer>()
                {{
                    put("lt", Int32Type.instance.getSerializer().deserialize(split[0]));
                    put("ge", Int32Type.instance.getSerializer().deserialize(split[1]));
                }});
            }
        }

        return results;
    }

    private void restart(Cluster cluster) throws Exception
    {
        cluster.get(1).shutdown().get();
        cluster.get(1).startup();
        coordinator = cluster.coordinator(1);
    }

    private void populateDatabase(Cluster cluster)
    {

        for (String stmt : SCHEMA_STATEMETS)
            cluster.schemaChange(stmt);

        insertData("Helsinki", "2018-09-13 21:00:00", 19.2);
        insertData("Boston", "2018-09-13 11:00:00", 27.2);
        insertData("Boston", "2018-09-13 20:00:00", 29.0);
        insertData("Boston", "2018-09-13 19:00:00", 28.8);
        insertData("Helsinki", "2018-09-13 23:00:00", 19.6);
        insertData("Boston", "2018-09-13 00:00:00", 25.0);
        insertData("Helsinki", "2018-09-13 06:00:00", 16.2);
        insertData("Boston", "2018-09-13 04:00:00", 25.8);
        insertData("Boston", "2018-09-13 01:00:00", 25.2);
        insertData("Boston", "2018-09-13 10:00:00", 27.0);
        insertData("Boston", "2018-09-13 21:00:00", 29.2);
        insertData("Helsinki", "2018-09-13 12:00:00", 17.4);
        insertData("Helsinki", "2018-09-13 20:00:00", 19.0);
        insertData("Helsinki", "2018-09-13 07:00:00", 16.4);
        insertData("Helsinki", "2018-09-13 08:00:00", 16.6);
        insertData("Helsinki", "2018-09-13 17:00:00", 18.4);
        insertData("Helsinki", "2018-09-13 18:00:00", 18.6);
        insertData("Helsinki", "2018-09-13 11:00:00", 17.2);
        insertData("Boston", "2018-09-13 12:00:00", 27.4);
        insertData("Helsinki", "2018-09-13 15:00:00", 18.0);
        insertData("Helsinki", "2018-09-13 01:00:00", 15.2);
        insertData("Helsinki", "2018-09-13 19:00:00", 18.8);
        insertData("Boston", "2018-09-13 15:00:00", 28.0);
        insertData("Helsinki", "2018-09-13 09:00:00", 16.8);
        insertData("Helsinki", "2018-09-13 05:00:00", 16.0);
        insertData("Helsinki", "2018-09-13 13:00:00", 17.6);
        insertData("Helsinki", "2018-09-13 22:00:00", 19.4);
        insertData("Boston", "2018-09-13 14:00:00", 27.8);
        insertData("Boston", "2018-09-13 22:00:00", 29.4);
        insertData("Boston", "2018-09-13 18:00:00", 28.6);
        insertData("Boston", "2018-09-13 23:00:00", 29.6);
        insertData("Boston", "2018-09-13 02:00:00", 25.4);
        insertData("Boston", "2018-09-13 05:00:00", 26.0);
        insertData("Boston", "2018-09-13 09:00:00", 26.8);
        insertData("Helsinki", "2018-09-13 04:00:00", 15.8);
        insertData("Helsinki", "2018-09-13 00:00:00", 15.0);
        insertData("Boston", "2018-09-13 17:00:00", 28.4);
        insertData("Helsinki", "2018-09-13 14:00:00", 17.8);
        insertData("Helsinki", "2018-09-13 02:00:00", 15.4);
        insertData("Helsinki", "2018-09-13 03:00:00", 15.6);
        insertData("Boston", "2018-09-13 08:00:00", 26.6);
        insertData("Boston", "2018-09-13 07:00:00", 26.4);
        insertData("Helsinki", "2018-09-13 16:00:00", 18.2);
        insertData("Boston", "2018-09-13 03:00:00", 25.6);
        insertData("Boston", "2018-09-13 16:00:00", 28.2);
        insertData("Boston", "2018-09-13 06:00:00", 26.2);
        insertData("Boston", "2018-09-13 13:00:00", 27.6);
        insertData("Helsinki", "2018-09-13 10:00:00", 17.0);
    }

    private void insertData(String city, String timestamp, double temp)
    {
        if (city.equals("Helsinki"))
            coordinator.execute(format(INSERT_TEMPLATE, KEYSPACE, city, timestamp, 60.1699, 24.9384, 51.0, temp), ALL);
        else
            coordinator.execute(format(INSERT_TEMPLATE, KEYSPACE, city, timestamp, 42.3601, -71.0589, 43.0, temp), ALL);
    }
}