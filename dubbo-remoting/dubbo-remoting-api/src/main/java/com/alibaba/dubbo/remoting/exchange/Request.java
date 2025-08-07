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

import com.alibaba.dubbo.common.utils.StringUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a request message in the exchange layer. (API, Prototype, ThreadSafe)
 * 
 * A request contains:
 * 1. Unique ID for request-response correlation
 * 2. Protocol version for compatibility
 * 3. Request type flags (two-way, event, broken)
 * 4. Actual request data
 * 
 * Special request types:
 * - Heartbeat request: Used for connection maintenance
 * - Readonly request: Marks the request as read-only operation
 * - Event request: Carries event data instead of regular request
 * 
 * Thread safety is achieved through:
 * - Immutable ID
 * - Atomic ID generation
 * - Thread-safe state flags
 * 
 * @see Response
 * @see ExchangeChannel
 */
public class Request {

    /**
     * Marker for heartbeat event requests.
     * When data equals this value, the request is considered a heartbeat.
     */
    public static final String HEARTBEAT_EVENT = null;

    /**
     * Marker for readonly event requests.
     * When data equals this value, the request is considered readonly.
     */
    public static final String READONLY_EVENT = "R";

    /**
     * Atomic counter for generating unique request IDs.
     * Uses atomic operation to ensure thread-safe ID generation.
     */
    private static final AtomicLong INVOKE_ID = new AtomicLong(0);

    /**
     * Unique identifier for this request.
     * Used for correlating requests with responses.
     */
    private final long mId;

    /**
     * Protocol version for compatibility.
     * Used to handle version differences between client and server.
     */
    private String mVersion;

    /**
     * Whether this request requires a response.
     * True for request-response pattern, false for one-way messages.
     */
    private boolean mTwoWay = true;

    /**
     * Whether this request is an event.
     * True for special events like heartbeat, false for regular requests.
     */
    private boolean mEvent = false;

    /**
     * Whether this request is broken.
     * True if there was an error during request creation or serialization.
     */
    private boolean mBroken = false;

    /**
     * The actual request data.
     * Can be regular request data, event data, or special markers.
     */
    private Object mData;

    /**
     * Creates a new request with an automatically generated ID.
     */
    public Request() {
        mId = newId();
    }

    /**
     * Creates a new request with a specified ID.
     * 
     * This constructor is typically used when reconstructing a request
     * from a serialized form where the ID is known.
     *
     * @param id The unique identifier for this request
     */
    public Request(long id) {
        mId = id;
    }

    /**
     * Generates a new unique request ID.
     * 
     * Uses atomic operation to ensure thread-safe ID generation.
     * When the counter reaches MAX_VALUE, it wraps around to MIN_VALUE,
     * allowing negative values to be used as valid IDs.
     *
     * @return A new unique request ID
     */
    private static long newId() {
        // getAndIncrement() When it grows to MAX_VALUE, it will grow to MIN_VALUE, and the negative can be used as ID
        return INVOKE_ID.getAndIncrement();
    }

    /**
     * Safely converts an object to its string representation.
     * 
     * This method handles the case where toString() might throw an exception:
     * - Returns null if the input is null
     * - Catches any exceptions during toString()
     * - Provides a fallback error message if toString() fails
     *
     * @param data The object to convert to string
     * @return The string representation or an error message
     */
    private static String safeToString(Object data) {
        if (data == null) return null;
        String dataStr;
        try {
            dataStr = data.toString();
        } catch (Throwable e) {
            dataStr = "<Fail toString of " + data.getClass() + ", cause: " +
                    StringUtils.toString(e) + ">";
        }
        return dataStr;
    }

    /**
     * Gets the unique identifier of this request.
     *
     * @return The request ID
     */
    public long getId() {
        return mId;
    }

    /**
     * Gets the protocol version of this request.
     *
     * @return The protocol version string
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Sets the protocol version of this request.
     *
     * @param version The protocol version string
     */
    public void setVersion(String version) {
        mVersion = version;
    }

    /**
     * Checks if this request requires a response.
     *
     * @return true if a response is expected, false for one-way requests
     */
    public boolean isTwoWay() {
        return mTwoWay;
    }

    /**
     * Sets whether this request requires a response.
     *
     * @param twoWay true to expect a response, false for one-way requests
     */
    public void setTwoWay(boolean twoWay) {
        mTwoWay = twoWay;
    }

    /**
     * Checks if this request is an event.
     *
     * @return true if this is an event request, false otherwise
     */
    public boolean isEvent() {
        return mEvent;
    }

    /**
     * Marks this request as an event and sets the event data.
     *
     * @param event The event data
     */
    public void setEvent(String event) {
        mEvent = true;
        mData = event;
    }

    /**
     * Checks if this request is broken.
     *
     * @return true if the request is broken, false otherwise
     */
    public boolean isBroken() {
        return mBroken;
    }

    /**
     * Sets whether this request is broken.
     *
     * @param mBroken true if the request is broken, false otherwise
     */
    public void setBroken(boolean mBroken) {
        this.mBroken = mBroken;
    }

    /**
     * Gets the request data.
     *
     * @return The request data object
     */
    public Object getData() {
        return mData;
    }

    /**
     * Sets the request data.
     *
     * @param msg The request data object
     */
    public void setData(Object msg) {
        mData = msg;
    }

    /**
     * Checks if this request is a heartbeat event.
     *
     * @return true if this is a heartbeat request, false otherwise
     */
    public boolean isHeartbeat() {
        return mEvent && HEARTBEAT_EVENT == mData;
    }

    /**
     * Sets whether this request is a heartbeat event.
     * If true, converts the request into a heartbeat event.
     *
     * @param isHeartbeat true to make this a heartbeat request
     */
    public void setHeartbeat(boolean isHeartbeat) {
        if (isHeartbeat) {
            setEvent(HEARTBEAT_EVENT);
        }
    }

    @Override
    public String toString() {
        return "Request [id=" + mId + ", version=" + mVersion + ", twoway=" + mTwoWay + ", event=" + mEvent
                + ", broken=" + mBroken + ", data=" + (mData == this ? "this" : safeToString(mData)) + "]";
    }
}
