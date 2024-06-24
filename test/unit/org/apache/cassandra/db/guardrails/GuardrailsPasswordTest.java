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

package org.apache.cassandra.db.guardrails;

import java.util.Map;

import org.junit.Test;

import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_WARN_KEY;

public class GuardrailsPasswordTest extends GuardrailTester
{
    private void setGuardrail(Map<String, Object> config)
    {
        Guardrails.instance.reconfigurePasswordValidator(config);
    }

    @Test
    public void testPasswordGuardrail()
    {
        // test that by default there is no password guardrail

        setGuardrail(getConfig());

        // disable password guardrail

        // test that there is no password guardrail in effect again
    }

    private CustomGuardrailConfig getConfig()
    {
        CustomGuardrailConfig config = new CassandraPasswordConfiguration(new CustomGuardrailConfig()).asCustomGuardrailConfig();

        config.put(ValueValidator.CLASS_NAME_KEY, CassandraPasswordValidator.class.getName());
        config.put(ValueGenerator.GENERATOR_CLASS_NAME_KEY, CassandraPasswordGenerator.class.getName());

        config.put(MIN_LENGTH_FAIL_KEY, 15);
        config.put(MIN_LENGTH_WARN_KEY, 20);

        return config;
    }
}
