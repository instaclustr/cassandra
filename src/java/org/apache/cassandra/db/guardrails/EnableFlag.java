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

import java.util.function.Predicate;
import javax.annotation.Nullable;

import org.apache.cassandra.service.ClientState;

/**
 * A guardrail that enables the use of a particular feature.
 *
 * <p>Note that this guardrail only aborts operations (if the feature is not enabled) so is only meant for query-based
 * guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail that breaks
 * compaction for instance).
 */
public class EnableFlag extends Guardrail
{
    private final Predicate<ClientState> enabled;
    private final String featureName;
    private final boolean alwaysWarn;

    /**
     * Creates a new {@link EnableFlag} guardrail.
     *
     * @param name        the identifying name of the guardrail
     * @param reason      the optional description of the reason for guarding the operation
     * @param enabled     a {@link ClientState}-based supplier of boolean indicating whether the feature guarded by this
     *                    guardrail is enabled.
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages), {@link
     *                    EnableFlag#ensureEnabled(String, ClientState)} can specify a different {@code featureName}.
     */
    public EnableFlag(String name, @Nullable String reason, Predicate<ClientState> enabled, String featureName)
    {
        this(name, reason, enabled, featureName, false);
    }

    /**
     * Creates a new {@link EnableFlag} guardrail.
     *
     * @param name        the identifying name of the guardrail
     * @param reason      the optional description of the reason for guarding the operation
     * @param enabled     a {@link ClientState}-based supplier of boolean indicating whether the feature guarded by this
     *                    guardrail is enabled.
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages), {@link
     *                    EnableFlag#ensureEnabled(String, ClientState)} can specify a different {@code featureName}.
     * @param alwaysWarn  If set to true, when this guardrail does not fail, it will always emit a warning. If it is
     *                    set to false, when this guardrail does not fail, it will not emit any warning. This might
     *                    be used for guardrails which are disabled, but we still want to inform a user that such a
     *                    feature or combination of configuration properties is suspicious, and a user should take
     *                    extra care to be sure it is indeed what is desired.
     */
    public EnableFlag(String name, @Nullable String reason, Predicate<ClientState> enabled, String featureName, boolean alwaysWarn)
    {
        super(name, reason);
        this.enabled = enabled;
        this.featureName = featureName;
        this.alwaysWarn = alwaysWarn;
    }

    /**
     * Returns whether the guarded feature is enabled or not.
     *
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     * @return {@code true} is the feature is enabled, {@code false} otherwise.
     */
    public boolean isEnabled(@Nullable ClientState state)
    {
        return !enabled(state) || enabled.test(state);
    }

    /**
     * Aborts the operation if this guardrail is not enabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param state The client state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    public void ensureEnabled(@Nullable ClientState state)
    {
        ensureEnabled(featureName, state);
    }

    /**
     * Aborts the operation if this guardrail is not enabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param featureName The feature that is guarded by this guardrail (for reporting in error messages).
     * @param state       The client state, used to skip the check if the query is internal or is done by a superuser. A
     *                    {@code null} value means that the check should be done regardless of the query, although it
     *                    won't throw any exception if the failure threshold is exceeded. This is so because checks
     *                    without an associated client come from asynchronous processes such as compaction, and we don't
     *                    want to interrupt such processes.
     */
    public void ensureEnabled(String featureName, @Nullable ClientState state)
    {
        if (!isEnabled(state))
            fail(featureName + " is not allowed", state);

        if (alwaysWarn)
            warn("Beware using " + featureName);
    }
}
