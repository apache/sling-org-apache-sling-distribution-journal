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
package org.apache.sling.distribution.journal.msg;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.Reset;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;

public class InMemoryTopic {
    private final String topicName;
    private final AtomicLong latestOffset;
    private final long earliestOffset;
    private final NavigableMap<Long, FullMessage<?>> messages = new ConcurrentSkipListMap<>();
    
    public InMemoryTopic(String topicName) {
        this.topicName = topicName;
        this.earliestOffset = 0;
        this.latestOffset = new AtomicLong(0);
    }
    
    public void setLatestOffset(long latestOffset) {
        this.latestOffset.set(latestOffset);
    }
    
    public void send(Object msg) {
        long offset = latestOffset.getAndIncrement();
        TestMessageInfo info = new TestMessageInfo(topicName, 0, offset, System.currentTimeMillis());
        FullMessage<?> fullMsg = new FullMessage<>(info, msg);
        messages.put(fullMsg.getInfo().getOffset(), fullMsg);
    }

    public long getOffset(Reset reset, String assign) {
        if (assign != null) {
            String[] parts = assign.split(":");
            return Long.parseLong(parts[1]) - 1;
        }
        if (reset == Reset.earliest) {
            return earliestOffset - 1;
        } else if (reset == Reset.latest) {
            return latestOffset.get();
        } else {
            throw new IllegalArgumentException("Must supply reset");
        }
    }

    public FullMessage<?> next(long curOffset) {
        Entry<Long, FullMessage<?>> entry = messages.ceilingEntry(curOffset + 1);
        return entry != null ? entry.getValue() : null;
    }
}
