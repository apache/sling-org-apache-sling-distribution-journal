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
package org.apache.sling.distribution.journal.impl.queue;

public class QueueState {
    
    private final long lastProcessedOffset;
    private final int headRetries;
    private final int maxRetries;
    private final ClearCallback clearCallback;

    public QueueState(long lastProcessedOffset, int headRetries, int maxRetries, ClearCallback clearCallback) {
        this.lastProcessedOffset = lastProcessedOffset;
        this.headRetries = headRetries;
        this.maxRetries = maxRetries;
        this.clearCallback = clearCallback;
    }
    
    public long getLastProcessedOffset() {
        return lastProcessedOffset;
    }
    
    public int getHeadRetries() {
        return headRetries;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }

    public ClearCallback getClearCallback() {
        return clearCallback;
    }
}
