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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.Test;

import org.apache.cassandra.db.guardrails.ValueValidator.ValidationViolation;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_ILLEGAL_SEQUENCE_LENGTH;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_CHARACTERISTICS_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_CHARACTERISTICS_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_LENGTH_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_LENGTH_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_LOWER_CASE_CHARS_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_LOWER_CASE_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_SPECIAL_CHARS_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_SPECIAL_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_UPPER_CASE_CHARS_FAIL;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.DEFAULT_MIN_UPPER_CASE_CHARS_WARN;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.ILLEGAL_SEQUENCE_LENGTH_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MAX_CHARACTERISTICS;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_CHARACTERISTICS_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_CHARACTERISTICS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_DIGITS_CHARS_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_DIGITS_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LENGTH_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LOWER_CASE_CHARS_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_LOWER_CASE_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_SPECIAL_CHARS_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_SPECIAL_CHARS_WARN_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_UPPER_CASE_CHARS_FAIL_KEY;
import static org.apache.cassandra.db.guardrails.CassandraPasswordConfiguration.MIN_UPPER_CASE_CHARS_WARN_KEY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CassandraPasswordValidatorTest
{
    @Test
    public void testEmptyConfigIsValid()
    {
        new CassandraPasswordValidator(new CustomGuardrailConfig());
    }

    @Test
    public void testDefaultConfiguration()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        CustomGuardrailConfig parameters = validator.getParameters();
        CassandraPasswordConfiguration conf = new CassandraPasswordConfiguration(parameters);

        assertEquals(DEFAULT_MIN_CHARACTERISTICS_WARN, conf.minCharacteristicsWarn);
        assertEquals(DEFAULT_MIN_CHARACTERISTICS_FAIL, conf.minCharacteristicsFail);

        assertEquals(DEFAULT_MIN_LENGTH_WARN, conf.minLengthWarn);
        assertEquals(DEFAULT_MIN_LENGTH_FAIL, conf.minLengthFail);

        assertEquals(DEFAULT_MIN_UPPER_CASE_CHARS_WARN, conf.minUpperCaseCharsWarn);
        assertEquals(DEFAULT_MIN_UPPER_CASE_CHARS_FAIL, conf.minUpperCaseCharsFail);

        assertEquals(DEFAULT_MIN_LOWER_CASE_CHARS_WARN, conf.minLowerCaseCharsWarn);
        assertEquals(DEFAULT_MIN_LOWER_CASE_CHARS_FAIL, conf.minLowerCaseCharsFail);

        assertEquals(DEFAULT_MIN_SPECIAL_CHARS_WARN, conf.minSpecialCharsWarn);
        assertEquals(DEFAULT_MIN_SPECIAL_CHARS_FAIL, conf.minSpecialCharsFail);

        assertEquals(DEFAULT_ILLEGAL_SEQUENCE_LENGTH, conf.illegalSequenceLength);
    }

    @Test
    public void testInvalidParameters()
    {
        for (int[] warnAndFail : new int[][]{ { 10, 10 }, { 5, 10 } })
        {
            int warn = warnAndFail[0];
            int fail = warnAndFail[1];
            validateWithConfig(() -> Map.of(MIN_LENGTH_WARN_KEY, warn, MIN_LENGTH_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      MIN_LENGTH_WARN_KEY, warn, MIN_LENGTH_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(MIN_SPECIAL_CHARS_WARN_KEY, warn, MIN_SPECIAL_CHARS_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      MIN_SPECIAL_CHARS_WARN_KEY, warn, MIN_SPECIAL_CHARS_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(MIN_DIGITS_CHARS_WARN_KEY, warn, MIN_DIGITS_CHARS_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      MIN_DIGITS_CHARS_WARN_KEY, warn, MIN_DIGITS_CHARS_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(MIN_LOWER_CASE_CHARS_WARN_KEY, warn, MIN_LOWER_CASE_CHARS_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      MIN_LOWER_CASE_CHARS_WARN_KEY, warn, MIN_LOWER_CASE_CHARS_FAIL_KEY, fail));

            validateWithConfig(() -> Map.of(MIN_UPPER_CASE_CHARS_WARN_KEY, warn, MIN_UPPER_CASE_CHARS_FAIL_KEY, fail),
                               format("%s of value %s is less or equal to %s of value %s",
                                      MIN_UPPER_CASE_CHARS_WARN_KEY, warn, MIN_UPPER_CASE_CHARS_FAIL_KEY, fail));
        }

        validateWithConfig(() -> Map.of(ILLEGAL_SEQUENCE_LENGTH_KEY, 3),
                           format("Illegal sequence length can not be lower than %s.",
                                  DEFAULT_ILLEGAL_SEQUENCE_LENGTH));

        validateWithConfig(() -> Map.of(MIN_CHARACTERISTICS_WARN_KEY, 5),
                           format("%s can not be bigger than %s",
                                  MIN_CHARACTERISTICS_WARN_KEY, MAX_CHARACTERISTICS));

        validateWithConfig(() -> Map.of(MIN_CHARACTERISTICS_FAIL_KEY, 5),
                           format("%s can not be bigger than %s",
                                  MIN_CHARACTERISTICS_FAIL_KEY, MAX_CHARACTERISTICS));

        validateWithConfig(() -> Map.of(MIN_CHARACTERISTICS_WARN_KEY, 3, MIN_CHARACTERISTICS_FAIL_KEY, 3),
                           format("%s can not be equal to %s. You set %s and %s respectively.",
                                  MIN_CHARACTERISTICS_FAIL_KEY, MIN_CHARACTERISTICS_WARN_KEY, 3, 3));

        validateWithConfig(() -> Map.of(MIN_CHARACTERISTICS_WARN_KEY, 3, MIN_CHARACTERISTICS_FAIL_KEY, 4),
                           format("%s can not be bigger than %s. You have set %s and %s respectively.",
                                  MIN_CHARACTERISTICS_FAIL_KEY, MIN_CHARACTERISTICS_WARN_KEY, 4, 3));

        validateWithConfig(() -> Map.of(MIN_SPECIAL_CHARS_WARN_KEY, 1,
                                        MIN_SPECIAL_CHARS_FAIL_KEY, 0,
                                        MIN_DIGITS_CHARS_WARN_KEY, 1,
                                        MIN_DIGITS_CHARS_FAIL_KEY, 0,
                                        MIN_UPPER_CASE_CHARS_WARN_KEY, 2,
                                        MIN_LOWER_CASE_CHARS_WARN_KEY, 2,
                                        MIN_CHARACTERISTICS_WARN_KEY, 3,
                                        MIN_CHARACTERISTICS_FAIL_KEY, 2,
                                        MIN_LENGTH_WARN_KEY, 3,
                                        MIN_LENGTH_FAIL_KEY, 2),
                           format("The shortest password to pass the warning validator for any %s characteristics out of %s is %s but you have set the %s to %s.",
                                  3, MAX_CHARACTERISTICS, 4, MIN_LENGTH_WARN_KEY, 3));

        validateWithConfig(() ->
                           new HashMap<>()
                           {{
                               put(MIN_SPECIAL_CHARS_FAIL_KEY, 1);
                               put(MIN_DIGITS_CHARS_WARN_KEY, 2);
                               put(MIN_DIGITS_CHARS_FAIL_KEY, 1);
                               put(MIN_UPPER_CASE_CHARS_WARN_KEY, 2);
                               put(MIN_UPPER_CASE_CHARS_FAIL_KEY, 1);
                               put(MIN_LOWER_CASE_CHARS_WARN_KEY, 2);
                               put(MIN_LOWER_CASE_CHARS_FAIL_KEY, 1);
                               put(MIN_CHARACTERISTICS_WARN_KEY, 4);
                               put(MIN_CHARACTERISTICS_FAIL_KEY, 3);
                               put(MIN_LENGTH_WARN_KEY, 8);
                               put(MIN_LENGTH_FAIL_KEY, 2);
                           }},
                           format("The shortest password to pass the failing validator for any %s characteristics out of %s is %s but you have set the %s to %s.",
                                  3, MAX_CHARACTERISTICS, 3, MIN_LENGTH_FAIL_KEY, 2));
    }


    @Test
    public void testIllegalSequences()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldFail("A1$abcdefgh");
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message, containsString("Password contains the illegal alphabetical sequence 'abcdefgh'."));

        validationResult = validator.shouldFail("A1$a123456");
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message, containsString("Password contains the illegal numerical sequence '123456'."));

        validationResult = validator.shouldFail("A1$asdfghjkl");
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message, containsString("Password contains the illegal QWERTY sequence 'asdfghjkl'."));
    }

    @Test
    public void testWhitespace()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldFail("A1$abcd efgh");
        assertTrue(validationResult.isPresent());
        assertThat(validationResult.get().message, containsString("Password contains a whitespace character."));
    }

    @Test
    public void testFailingValidationResult()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldFail("acefghuiiui");
        assertTrue(validationResult.isPresent());
        assertEquals("[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, INSUFFICIENT_SPECIAL, INSUFFICIENT_UPPERCASE]",
                     validationResult.get().redactedMessage);
        assertEquals("Password was not set as it violated configured password strength policy. " +
                     "To fix this error, the following has to be resolved: " +
                     "Password must contain 1 or more uppercase characters. " +
                     "Password must contain 1 or more digit characters. " +
                     "Password must contain 1 or more special characters. " +
                     "Password matches 1 of 4 character rules, but 2 are required. ",
                     validationResult.get().message);
    }

    @Test
    public void testWarningValidationResult()
    {
        CassandraPasswordValidator validator = new CassandraPasswordValidator(new CustomGuardrailConfig());
        Optional<ValidationViolation> validationResult = validator.shouldWarn("t$Efg1#a..fr");
        assertTrue(validationResult.isPresent());
        assertEquals("[INSUFFICIENT_DIGIT, INSUFFICIENT_CHARACTERISTICS, INSUFFICIENT_UPPERCASE]",
                     validationResult.get().redactedMessage);
        assertEquals("Password was set, however it might not be strong enough according to the " +
                     "configured password strength policy. To fix this warning, the following has to be resolved: " +
                     "Password must contain 2 or more uppercase characters. " +
                     "Password must contain 2 or more digit characters. " +
                     "Password matches 2 of 4 character rules, but 3 are required. ",
                     validationResult.get().message);
    }

    private void validateWithConfig(Supplier<Map<String, Object>> configSupplier, String expectedMessage)
    {
        CustomGuardrailConfig customConfig = new CustomGuardrailConfig();
        customConfig.putAll(configSupplier.get());

        assertThatThrownBy(() -> new CassandraPasswordConfiguration(customConfig))
        .hasMessageContaining(expectedMessage)
        .isInstanceOf(ConfigurationException.class);
    }
}
