/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.remoting.exchange;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a response message in the exchange layer. (API, Prototype, ThreadSafe)
 * 
 * A response contains:
 * 1. ID that correlates with the request
 * 2. Status code indicating success/failure
 * 3. Result data or error message
 * 4. Protocol version for compatibility
 * 
 * Response status codes are categorized as:
 * - 20~29: Success responses
 * - 30~39: Client/Server timeouts
 * - 40~49: Bad request errors
 * - 50~59: Bad response errors
 * - 60~69: Service not found
 * - 70~79: Service errors
 * - 80~89: Server errors
 * - 90~99: Client errors
 * - 100+: Special errors (e.g., threadpool exhausted)
 * 
 * Special response types:
 * - Heartbeat response: Used for connection maintenance
 * - Event response: Carries event data instead of regular response
 * 
 * @see Request
 * @see ExchangeChannel
 */
public class Response {

    /**
     * Marker for heartbeat event responses.
     * When result equals this value, the response is considered a heartbeat.
     */
    public static final String HEARTBEAT_EVENT = null;

    /**
     * Marker for readonly event responses.
     * When result equals this value, the response is considered readonly.
     */
    public static final String READONLY_EVENT = "R";

    /**
     * Status code for successful response.
     * Indicates the request was processed successfully.
     */
    public static final byte OK = 20;

    /**
     * Status code for client-side timeout.
     * Indicates the client gave up waiting for the response.
     */
    public static final byte CLIENT_TIMEOUT = 30;

    /**
     * Status code for server-side timeout.
     * Indicates the server took too long to process the request.
     */
    public static final byte SERVER_TIMEOUT = 31;

    /**
     * Status code for inactive channel.
     * Indicates the connection was lost before the response could be sent.
     */
    public static final byte CHANNEL_INACTIVE = 35;

    /**
     * Status code for malformed request.
     * Indicates the request was invalid or could not be understood.
     */
    public static final byte BAD_REQUEST = 40;

    /**
     * Status code for malformed response.
     * Indicates the response was corrupted or invalid.
     */
    public static final byte BAD_RESPONSE = 50;

    /**
     * Status code for service not found.
     * Indicates the requested service does not exist.
     */
    public static final byte SERVICE_NOT_FOUND = 60;

    /**
     * Status code for service error.
     * Indicates an error occurred while processing the request.
     */
    public static final byte SERVICE_ERROR = 70;

    /**
     * Status code for internal server error.
     * Indicates an unexpected error occurred on the server.
     */
    public static final byte SERVER_ERROR = 80;

    /**
     * Status code for client error.
     * Indicates an error occurred on the client side.
     */
    public static final byte CLIENT_ERROR = 90;

    /**
     * Status code for server threadpool exhaustion.
     * Indicates the server's thread pool is full and cannot process the request.
     */
    public static final byte SERVER_THREADPOOL_EXHAUSTED_ERROR = 100;

    /**
     * The ID of the request this response corresponds to.
     * Used for request-response correlation.
     */
    private long mId = 0;

    /**
     * Protocol version for compatibility.
     * Should match the request version.
     */
    private String mVersion;

    /**
     * Response status code indicating success/failure.
     * Default is OK (20).
     */
    private byte mStatus = OK;

    /**
     * Whether this response is an event.
     * True for special events like heartbeat.
     */
    private boolean mEvent = false;

    /**
     * Error message when status indicates failure.
     * Contains details about what went wrong.
     */
    private String mErrorMsg;

    /**
     * The actual response data.
     * Contains the result for successful responses.
     */
    private Object mResult;

    /**
     * Additional attributes for this response.
     * Used for extending the response with custom data.
     */
    private Map<String, Object> attributes = new HashMap<String, Object>(2);

    /**
     * Creates a new response with default values.
     */
    public Response() {
    }

    /**
     * Creates a new response with the specified ID.
     *
     * @param id The ID that correlates with the request
     */
    public Response(long id) {
        mId = id;
    }

    /**
     * Creates a new response with the specified ID and version.
     *
     * @param id The ID that correlates with the request
     * @param version The protocol version
     */
    public Response(long id, String version) {
        mId = id;
        mVersion = version;
    }

    /**
     * Gets the response ID.
     *
     * @return The ID that correlates with the request
     */
    public long getId() {
        return mId;
    }

    /**
     * Sets the response ID.
     *
     * @param id The ID that correlates with the request
     */
    public void setId(long id) {
        mId = id;
    }

    /**
     * Gets the protocol version.
     *
     * @return The protocol version string
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Sets the protocol version.
     *
     * @param version The protocol version string
     */
    public void setVersion(String version) {
        mVersion = version;
    }

    /**
     * Gets the response status code.
     *
     * @return The status code indicating success/failure
     */
    public byte getStatus() {
        return mStatus;
    }

    /**
     * Sets the response status code.
     *
     * @param status The status code indicating success/failure
     */
    public void setStatus(byte status) {
        mStatus = status;
    }

    /**
     * Checks if this response is an event.
     *
     * @return true if this is an event response, false otherwise
     */
    public boolean isEvent() {
        return mEvent;
    }

    /**
     * Marks this response as an event and sets the event data.
     *
     * @param event The event data
     */
    public void setEvent(String event) {
        mEvent = true;
        mResult = event;
    }

    /**
     * Checks if this response is a heartbeat event.
     *
     * @return true if this is a heartbeat response, false otherwise
     */
    public boolean isHeartbeat() {
        return mEvent && HEARTBEAT_EVENT == mResult;
    }

    /**
     * Sets whether this response is a heartbeat event.
     * 
     * @deprecated Use setEvent(HEARTBEAT_EVENT) instead
     * @param isHeartbeat true to make this a heartbeat response
     */
    @Deprecated
    public void setHeartbeat(boolean isHeartbeat) {
        if (isHeartbeat) {
            setEvent(HEARTBEAT_EVENT);
        }
    }

    /**
     * Gets the response result.
     *
     * @return The result object for successful responses
     */
    public Object getResult() {
        return mResult;
    }

    /**
     * Sets the response result.
     *
     * @param msg The result object for successful responses
     */
    public void setResult(Object msg) {
        mResult = msg;
    }

    /**
     * Gets the error message.
     *
     * @return The error message for failed responses
     */
    public String getErrorMessage() {
        return mErrorMsg;
    }

    /**
     * Sets the error message.
     *
     * @param msg The error message for failed responses
     */
    public void setErrorMessage(String msg) {
        mErrorMsg = msg;
    }

    /**
     * Gets a custom attribute value.
     *
     * @param key The attribute key
     * @return The attribute value, or null if not found
     */
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    /**
     * Sets a custom attribute.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public String toString() {
        return "Response [id=" + mId + ", version=" + mVersion + ", status=" + mStatus + ", event=" + mEvent
                + ", error=" + mErrorMsg + ", result=" + (mResult == this ? "this" : mResult) + "]";
    }
}