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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.passay.CharacterCharacteristicsRule;
import org.passay.CharacterData;
import org.passay.CharacterRule;
import org.passay.IllegalSequenceRule;
import org.passay.LengthRule;
import org.passay.PasswordData;
import org.passay.PasswordValidator;
import org.passay.Rule;
import org.passay.RuleResult;
import org.passay.RuleResultDetail;
import org.passay.WhitespaceRule;

import static java.util.Optional.empty;
import static org.passay.EnglishCharacterData.Digit;
import static org.passay.EnglishCharacterData.LowerCase;
import static org.passay.EnglishCharacterData.UpperCase;
import static org.passay.EnglishSequenceData.Alphabetical;
import static org.passay.EnglishSequenceData.Numerical;
import static org.passay.EnglishSequenceData.USQwerty;

public class CassandraPasswordValidator extends ValueValidator<String>
{
    protected static final CharacterData specialCharacters = new CharacterData()
    {
        @Override
        public String getErrorCode()
        {
            return "INSUFFICIENT_SPECIAL";
        }

        @Override
        public String getCharacters()
        {
            return "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
        }
    };

    protected final PasswordValidator warningValidator;
    protected final PasswordValidator failingValidator;

    private final CassandraPasswordConfiguration configuration;

    public CassandraPasswordValidator(CustomGuardrailConfig config)
    {
        super(config);
        configuration = new CassandraPasswordConfiguration(config);

        warningValidator = new PasswordValidator(getRules(configuration.minLengthWarn,
                                                          configuration.minCharacteristicsWarn,
                                                          configuration.illegalSequenceLength,
                                                          getCharacterRules(configuration.minUpperCaseCharsWarn,
                                                                            configuration.minLowerCaseCharsWarn,
                                                                            configuration.minDigitsCharsWarn,
                                                                            configuration.minSpecialCharsWarn)));
        failingValidator = new PasswordValidator(getRules(configuration.minLengthFail,
                                                          configuration.minCharacteristicsFail,
                                                          configuration.illegalSequenceLength,
                                                          getCharacterRules(configuration.minUpperCaseCharsFail,
                                                                            configuration.minLowerCaseCharsFail,
                                                                            configuration.minDigitsCharsFail,
                                                                            configuration.minSpecialCharsFail)));
    }


    @Nonnull
    @Override
    public CustomGuardrailConfig getParameters()
    {
        return configuration.asCustomGuardrailConfig();
    }

    @Override
    public Optional<ValidationViolation> shouldWarn(String newValue)
    {
        RuleResult result = warningValidator.validate(new PasswordData(newValue));
        return result.isValid() ? empty() : Optional.of(getValidationMessage(warningValidator, true, result));
    }

    @Override
    public Optional<ValidationViolation> shouldFail(String newValue)
    {
        RuleResult result = failingValidator.validate(new PasswordData(newValue));
        return result.isValid() ? empty() : Optional.of(getValidationMessage(failingValidator, false, result));
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        configuration.validateParameters();
    }

    public static List<CharacterRule> getCharacterRules(int upper, int lower, int digits, int special)
    {
        return Arrays.asList(new CharacterRule(UpperCase, upper),
                             new CharacterRule(LowerCase, lower),
                             new CharacterRule(Digit, digits),
                             new CharacterRule(specialCharacters, special));
    }

    private List<Rule> getRules(int length,
                                int characteristics,
                                int illegalSequenceLength,
                                List<CharacterRule> characterRules)
    {
        List<Rule> rules = new ArrayList<>();

        rules.add(new LengthRule(length, Integer.MAX_VALUE));

        CharacterCharacteristicsRule characteristicsRule = new CharacterCharacteristicsRule();
        characteristicsRule.setNumberOfCharacteristics(characteristics);
        characteristicsRule.getRules().addAll(characterRules);
        rules.add(characteristicsRule);

        rules.add(new WhitespaceRule());

        rules.add(new IllegalSequenceRule(Alphabetical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(Numerical, illegalSequenceLength, false));
        rules.add(new IllegalSequenceRule(USQwerty, illegalSequenceLength, false));

        return rules;
    }

    private ValidationViolation getValidationMessage(PasswordValidator validator, boolean forWarn, RuleResult result)
    {
        String type = forWarn ? "warning" : "error";
        StringBuilder sb = new StringBuilder();
        sb.append("Password was")
          .append(forWarn ? " set, however it might not be strong enough according to the configured password strength policy. "
                          : " not set as it violated configured password strength policy. ")
          .append("To fix this ")
          .append(type)
          .append(", the following has to be resolved: ");

        for (String message : validator.getMessages(result))
            sb.append(message).append(' ');

        String message = sb.toString();

        Set<String> errorCodes = new HashSet<>();
        for (RuleResultDetail ruleResultDetail : result.getDetails())
            errorCodes.add(ruleResultDetail.getErrorCode());

        String redactedMessage = errorCodes.toString();

        return new ValidationViolation(message, redactedMessage);
    }
}
