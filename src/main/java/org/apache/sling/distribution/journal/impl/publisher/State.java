/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sling.distribution.journal.impl.publisher;

import java.util.Objects;

import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.ToStringBuilder;

import static org.apache.commons.lang3.builder.ToStringStyle.JSON_STYLE;

@Immutable
@ParametersAreNonnullByDefault
public class State {

    private final long timestamp;

    private final long offset;

    private final int retries;

    private final String pubAgentName;

    private final String subAgentId;

    private final boolean editable;

    private final int maxRetries;

    public State(String pubAgentName, String subAgentId, long timestamp, long offset, int retries, int maxRetries, boolean editable) {
        this.pubAgentName = pubAgentName;
        this.subAgentId = subAgentId;
        this.timestamp = timestamp;
        this.offset = offset;
        this.retries = retries;
        this.maxRetries = maxRetries;
        this.editable = editable;
    }

    public String getPubAgentName() {
        return pubAgentName;
    }
    
    public String getSubAgentId() {
        return subAgentId;
    }

    public long getOffset() {
        return offset;
    }

    public int getRetries() {
        return retries;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isEditable() {
        return editable;
    }

    /**
     * Indicates whether some other State is "equal to" this one.
     *
     * Equality compares all members except the #timestamp.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        State state = (State) o;
        return offset == state.offset &&
                retries == state.retries &&
                maxRetries == state.maxRetries &&
                editable == state.editable &&
                Objects.equals(pubAgentName, state.pubAgentName) &&
                Objects.equals(subAgentId, state.subAgentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, retries, maxRetries, editable, pubAgentName, subAgentId);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, JSON_STYLE)
                .append("timestamp", timestamp)
                .append("offset", offset)
                .append("retries", retries)
                .append("maxRetries", maxRetries)
                .append("editable", editable)
                .append("pubAgentName", pubAgentName)
                .append("subAgentId", subAgentId)
                .toString();
    }
}