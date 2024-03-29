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

import java.util.Map;

import org.apache.sling.distribution.journal.MessageSender;
import org.apache.sling.distribution.journal.MessagingException;

public class InMemorySender<T> implements MessageSender<T> {

    private InMemoryTopic topic;

    public InMemorySender(InMemoryTopic topic) {
        this.topic = topic;
    }

    @Override
    public void send(T payload) throws MessagingException {
        topic.send(payload);
    }

    @Override
    public void send(T payload, Map<String, String> properties) throws MessagingException {
        topic.send(payload);
    }

}
