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

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.sling.distribution.journal.HandlerAdapter;
import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

public class InMemoryProvider implements MessagingProvider {
    
    Map<String, InMemoryTopic> topics = new ConcurrentHashMap<>();
    
    public InMemoryProvider() {
    }

    @Override
    public <T> MessageSender<T> createSender(String topicName) {
        InMemoryTopic topic = getOrCreateTopic(topicName);
        return new InMemorySender<T>(topic);
    }

    @Override
    public Closeable createPoller(String topicName, Reset reset, String assign, HandlerAdapter<?>... adapters) {
        InMemoryTopic topic = getOrCreateTopic(topicName);
        return new InMemoryPoller(topic, reset, assign, adapters);
    }

    @Override
    public void assertTopic(String topic) throws MessagingException {
        
    }

    @Override
    public long retrieveOffset(String topicName, Reset reset) {
        return 0;
    }

    @Override
    public String assignTo(long offset) {
        return String.format("0:%d", offset);
    }

    @Override
    public URI getServerUri() {
        try {
            return new URI("http://localhost:12345/dummy");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void setLatestOffset(String topicName, long offset) {
        getOrCreateTopic(topicName).setLatestOffset(offset);
    }

    private InMemoryTopic getOrCreateTopic(String topicName) {
        return topics.computeIfAbsent(topicName, name -> new InMemoryTopic(name));
    }

}
