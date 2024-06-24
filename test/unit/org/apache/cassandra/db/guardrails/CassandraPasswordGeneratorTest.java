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

import java.util.List;

import org.junit.Test;

import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.Rule;
import org.passay.RuleResultDetail;

import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_WARN_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CassandraPasswordGeneratorTest
{
    @Test
    public void testPasswordGenerator()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();

        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);
        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);

        PasswordData passwordData = new PasswordData(generator.generate());
        for (Rule rule : validator.warningValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        for (Rule rule : validator.failingValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        assertTrue(validator.shouldWarn(passwordData.getPassword()).isEmpty());
        assertTrue(validator.shouldFail(passwordData.getPassword()).isEmpty());
    }

    @Test
    public void testPasswordGenerationLength()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(MIN_LENGTH_WARN_KEY, 20);
        config.put(MIN_LENGTH_FAIL_KEY, 15);

        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);

        assertEquals(20, generator.generate().length());
        assertEquals(30, generator.generate(30).length());
        assertEquals(18, generator.generate(18).length());
    }

    @Test
    public void testPasswordGenerationOfLengthViolatingThreshold()
    {
        CustomGuardrailConfig config = new CustomGuardrailConfig();
        config.put(MIN_LENGTH_WARN_KEY, 20);
        config.put(MIN_LENGTH_FAIL_KEY, 15);

        CassandraPasswordValidator validator = new CassandraPasswordValidator(config);
        CassandraPasswordGenerator generator = new CassandraPasswordGenerator(config);

        PasswordData passwordData = new PasswordData(generator.generate(18));

        for (Rule rule : validator.warningValidator.getRules())
        {
            if (rule instanceof LengthRule)
            {
                assertFalse(rule.validate(passwordData).isValid());
                List<RuleResultDetail> details = rule.validate(passwordData).getDetails();
                assertEquals(1, details.size());
                RuleResultDetail ruleResultDetail = details.get(0);
                assertEquals("TOO_SHORT", ruleResultDetail.getErrorCode());
            }
            else
            {
                assertTrue(rule.validate(passwordData).isValid());
            }
        }

        for (Rule rule : validator.failingValidator.getRules())
            assertTrue(rule.validate(passwordData).isValid());

        assertFalse(validator.shouldWarn(passwordData.getPassword()).isEmpty());
        assertTrue(validator.shouldFail(passwordData.getPassword()).isEmpty());
    }
}
