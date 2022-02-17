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

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.sling.distribution.DistributionRequestState;
import org.apache.sling.distribution.DistributionResponse;
import org.apache.sling.distribution.DistributionResponseInfo;

import static org.apache.sling.distribution.DistributionRequestState.ACCEPTED;
import static org.apache.sling.distribution.DistributionRequestState.DISTRIBUTED;
import static org.apache.sling.distribution.DistributionResponseInfo.NONE;

@ParametersAreNonnullByDefault
public final class SimpleDistributionResponse implements DistributionResponse {

    private final DistributionRequestState state;

    private final String message;
    
    private final DistributionResponseInfo info;

    public SimpleDistributionResponse(DistributionRequestState state, String message, DistributionResponseInfo info) {
        this.state = state;
        this.message = message;
        if (info == null) {
            info = NONE;
        }
        this.info = info;
    }

    public SimpleDistributionResponse(DistributionRequestState state, String message) {
        this(state, message, NONE);
    }

    @Override
    public boolean isSuccessful() {
        return state.equals(ACCEPTED) || state.equals(DISTRIBUTED);
    }

    @Nonnull
    @Override
    public DistributionRequestState getState() {
        return state;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public DistributionResponseInfo getDistributionInfo() {
        return info;
    }
}
