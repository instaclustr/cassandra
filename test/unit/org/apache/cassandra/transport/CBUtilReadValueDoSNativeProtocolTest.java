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
package org.apache.cassandra.transport;

import java.util.Objects;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Native-protocol reproducer for the pre-authentication unbounded-allocation DoS in
 * {@link CBUtil#readValue(ByteBuf)} — the realistic-attack counterpart to the unit-level
 * {@link CBUtilReadValueDoSTest}.
 *
 * <p>This test boots an in-process Cassandra {@link Server} via {@link
 * org.apache.cassandra.cql3.CQLTester#requireNetwork()}, opens a real TCP native-protocol
 * connection with {@link SimpleClient} on protocol V4 (the pre-V5 decoder path described
 * in the report), completes the STARTUP handshake, then sends an AUTH_RESPONSE whose body
 * is the 4-byte payload {@code 7f ff ff ff} — i.e. a SASL-token length of
 * {@link Integer#MAX_VALUE}. That mirrors the reporter's 13-byte AUTH_RESPONSE frame
 * from the netcat PoC.
 *
 * <p>Expected behaviour
 * <pre>
 *   On fixed code, CBUtil.readRawBytes rejects length &gt; cb.readableBytes() with a
 *   ProtocolException. The pre-V5 decoder catches it, wraps it in an ErrorMessage, and
 *   the client receives an ERROR whose payload contains the bound-check text
 *   "Cannot read value of length 2147483647". The server keeps running.
 * </pre>
 *
 * <p>Actual behaviour on unpatched code
 * <pre>
 *   CBUtil.readRawBytes does new byte[Integer.MAX_VALUE] and the JVM throws
 *   OutOfMemoryError ("Requested array size exceeds VM limit"). In a production
 *   Cassandra process with -XX:OnOutOfMemoryError=kill -9 %p the JVM is killed
 *   immediately. In this test JVM that flag is not set, so the OOM is caught in
 *   PreV5Handlers.ProtocolDecoder, wrapped as ErrorMessage, and returned to the
 *   client — but the response text references OutOfMemoryError, NOT the post-fix
 *   bound-check text, so the contains-assertion below fails.
 * </pre>
 *
 * <p>Side effect on unpatched code: because the test JVM is launched with
 * {@code -XX:+HeapDumpOnOutOfMemoryError} (see build.xml), the buggy run will
 * write a multi-hundred-MB {@code java_pid<pid>.hprof} dump file to the test
 * working directory as a JVM-level side effect of the OOM. No dump is produced
 * once the fix is in place.
 *
 * <p>Failure criterion (oracle)
 * <pre>
 *   The ERROR response payload must CONTAIN the substring
 *       "Cannot read value of length 2147483647"
 *   Any other response (different ERROR text, no response, timeout, or server crash)
 *   means the bug was not fixed at CBUtil.readRawBytes.
 * </pre>
 */
public class CBUtilReadValueDoSNativeProtocolTest extends NativeProtocolLimitsTestBase
{
    public CBUtilReadValueDoSNativeProtocolTest()
    {
        // V4 / pre-V5 path is the one the report and the reproducer netcat PoC exercise.
        super(ProtocolVersion.V4);
    }

    @BeforeClass
    public static void setUp()
    {
        requireNetwork();
    }

    // TRIGGER: AUTH_RESPONSE frame body = 4 bytes {0x7f, 0xff, 0xff, 0xff}, i.e. declared
    //          SASL-token length = Integer.MAX_VALUE, with no payload bytes following.
    //          Sent over a real TCP native-protocol connection after STARTUP, on V4.
    // HARNESS: in-process Cassandra Server (CQLTester.requireNetwork) + SimpleClient
    //          on 127.0.0.1:nativePort. The malicious body is injected by temporarily
    //          swapping AUTH_RESPONSE's codec via Message.Type#unsafeSetCodec — same
    //          mechanism UnableToParseClientMessageTest uses to send malformed bodies.
    // ORACLE:  the server's ErrorMessage response must contain the bound-check text
    //          "Cannot read value of length 2147483647". On unpatched code the response
    //          text references OutOfMemoryError instead, and the assertion fails.
    @Test
    public void authResponseWithUnboundedTokenLengthIsRejected()
    {
        ByteBuf maliciousBody = Unpooled.buffer(4);
        maliciousBody.writeInt(Integer.MAX_VALUE); // 0x7f ff ff ff

        try (SimpleClient client = client())
        {
            Message.Response response = client.execute(new CustomBodyMessage(Message.Type.AUTH_RESPONSE,
                                                                             maliciousBody),
                                                       false);

            Assertions.assertThat(response.type)
                      .as("Server must respond with an ERROR for the malformed AUTH_RESPONSE")
                      .isEqualTo(Message.Type.ERROR);
            Assertions.assertThat(((ErrorMessage) response).error.getMessage())
                      .as("ERROR payload must reference the CBUtil bound check, not OutOfMemoryError")
                      .contains("Cannot read value of length 2147483647");

            // Sanity-check the server is still alive after the malicious frame: a fresh
            // OPTIONS roundtrip on a new connection should succeed.
            try (SimpleClient probe = client())
            {
                Message.Response options = probe.execute(new OptionsMessage(), true);
                Assertions.assertThat(options.type).isEqualTo(Message.Type.SUPPORTED);
            }
        }
    }

    /**
     * Replaces AUTH_RESPONSE's encoder with one that writes a caller-supplied raw body, so
     * SimpleClient transmits exactly the bytes we want for the test. The server still uses
     * the original AuthResponse.Codec.decode on receive, which is what we want — that's the
     * code path under test.
     *
     * <p>Mirrors {@code UnableToParseClientMessageTest.CustomBodyMessage}.
     */
    private static final class CustomBodyMessage extends Message.Request
    {
        private final ByteBuf body;

        CustomBodyMessage(Message.Type type, ByteBuf body)
        {
            super(type);
            this.body = Objects.requireNonNull(body);
        }

        @Override
        public Envelope encode(ProtocolVersion version)
        {
            Message.Codec<?> originalCodec = type.codec;
            try
            {
                setCodec(type, new Message.Codec<Message>()
                {
                    @Override
                    public Message decode(ByteBuf in, ProtocolVersion v)
                    {
                        return originalCodec.decode(in, v);
                    }

                    @Override
                    public void encode(Message message, ByteBuf dest, ProtocolVersion v)
                    {
                        dest.writeBytes(body, body.readerIndex(), body.readableBytes());
                    }

                    @Override
                    public int encodedSize(Message message, ProtocolVersion v)
                    {
                        return body.readableBytes();
                    }
                });
                return super.encode(version);
            }
            finally
            {
                setCodec(type, originalCodec);
            }
        }

        @Override
        protected Message.Response execute(QueryState queryState,
                                           Dispatcher.RequestTime requestTime,
                                           boolean traceRequest)
        {
            throw new AssertionError("execute not expected for a malformed AUTH_RESPONSE");
        }
    }

    private static void setCodec(Message.Type type, Message.Codec<?> codec)
    {
        try
        {
            type.unsafeSetCodec(codec);
        }
        catch (NoSuchFieldException | IllegalAccessException e)
        {
            throw new AssertionError(e);
        }
    }
}
