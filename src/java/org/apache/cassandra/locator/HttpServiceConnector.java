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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

public class HttpServiceConnector
{
    public static final String METADATA_URL_PROPERTY = "metadata_url";
    public static final String METADATA_REQUEST_TIMEOUT_PROPERTY = "metadata_request_timeout";
    public static final String METADATA_REQUEST_HEADERS_PROPERTY = "metadata_request_headers";
    public static final String DEFAULT_METADATA_REQUEST_TIMEOUT = "30s";

    protected final String metadataServiceUrl;
    protected final int requestTimeoutMs;
    protected final Map<String, String> headers;

    public HttpServiceConnector(Properties properties)
    {
        this(resolveMetadataUrl(properties, METADATA_URL_PROPERTY),
             resolveRequestTimeoutMs(properties,
                                     HttpServiceConnector.METADATA_REQUEST_TIMEOUT_PROPERTY,
                                     HttpServiceConnector.DEFAULT_METADATA_REQUEST_TIMEOUT),
             resolveHeaders(properties));
    }

    public HttpServiceConnector(String serviceUrl, int requestTimeoutMs, Map<String, String> headers)
    {
        this.metadataServiceUrl = serviceUrl;
        this.requestTimeoutMs = requestTimeoutMs;
        this.headers = headers;
    }

    public final String apiCall(String query) throws IOException
    {
        return apiCall(metadataServiceUrl, query, "GET", headers, 200);
    }

    public final String apiCall(String query, Map<String, String> extraHeaders) throws IOException
    {
        Map<String, String> mergedHeaders = new HashMap<>(headers);
        mergedHeaders.putAll(extraHeaders);
        return apiCall(metadataServiceUrl, query, "GET", mergedHeaders, 200);
    }

    public String apiCall(String url,
                          String query,
                          String method,
                          Map<String, String> extraHeaders,
                          int expectedResponseCode) throws IOException
    {
        Map<String, String> mergedHeaders = new HashMap<>(headers);
        mergedHeaders.putAll(extraHeaders);
        HttpURLConnection conn = null;
        try
        {
            conn = (HttpURLConnection) new URL(url + query).openConnection();
            mergedHeaders.forEach(conn::setRequestProperty);
            conn.setRequestMethod(method);
            conn.setConnectTimeout(requestTimeoutMs);
            if (conn.getResponseCode() != expectedResponseCode)
                throw new HttpException(conn.getResponseCode(), conn.getResponseMessage());

            List<String> response = new ArrayList<>();

            try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(conn.getInputStream())))
            {
                String line;
                while ((line = bufferedReader.readLine()) != null)
                {
                    response.add(line);
                }
            }

            return response.stream().collect(Collectors.joining(System.lineSeparator()));
        }
        finally
        {
            if (conn != null)
                conn.disconnect();
        }
    }

    public static String resolveMetadataUrl(Properties properties, String keyProperty)
    {
        String parsedUrl = properties.getProperty(keyProperty);

        try
        {
            if (parsedUrl == null)
                throw new ConfigurationException(format("%s was not specified", keyProperty));

            URL url = new URL(parsedUrl);
            url.toURI();

            return parsedUrl;
        }
        catch (MalformedURLException | IllegalArgumentException | URISyntaxException ex)
        {
            throw new ConfigurationException(format("URL '%s' is invalid.", parsedUrl), ex);
        }
    }

    public static int resolveRequestTimeoutMs(Properties properties, String keyProperty, String defaultValue)
    {
        String metadataRequestTimeout = properties.getProperty(keyProperty, defaultValue);

        try
        {
            return new DurationSpec.IntMillisecondsBound(metadataRequestTimeout).toMilliseconds();
        }
        catch (IllegalArgumentException ex)
        {
            throw new ConfigurationException(format("%s as value of %s is invalid duration! " + ex.getMessage(),
                                                    metadataRequestTimeout,
                                                    keyProperty));
        }
    }

    public static Map<String, String> resolveHeaders(Properties properties)
    {
        String rawRequestHeaders = properties.getProperty(METADATA_REQUEST_HEADERS_PROPERTY);

        if (rawRequestHeaders == null || rawRequestHeaders.isBlank())
            return Collections.emptyMap();

        Map<String, String> headersMap = new HashMap<>();

        for (String rawHeaderPair : rawRequestHeaders.split(","))
        {
            String[] header = rawHeaderPair.trim().split("=");
            if (header.length != 2)
                continue;

            String headerKey = header[0].trim();
            String headerValue = header[1].trim();

            if (!headerKey.isEmpty() && !headerValue.isEmpty())
                headersMap.put(headerKey, headerValue);
        }

        return Collections.unmodifiableMap(headersMap);
    }

    @Override
    public String toString()
    {
        return format("%s{%s=%s,%s=%s,%s=%s}", getClass().getName(),
                      METADATA_URL_PROPERTY, metadataServiceUrl,
                      METADATA_REQUEST_TIMEOUT_PROPERTY, requestTimeoutMs,
                      METADATA_REQUEST_HEADERS_PROPERTY, headers);
    }

    public static final class HttpException extends IOException
    {
        public final int responseCode;
        public final String responseMessage;

        public HttpException(int responseCode, String responseMessage)
        {
            super("HTTP response code: " + responseCode + " (" + responseMessage + ')');
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
        }
    }
}
