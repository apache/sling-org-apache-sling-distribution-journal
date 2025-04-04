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
package org.apache.sling.distribution.journal.queue;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageHandler;
import org.apache.sling.distribution.journal.impl.discovery.State;
import org.apache.sling.distribution.journal.messages.PackageMessage;

public interface CacheCallback {
    Closeable createConsumer(MessageHandler<PackageMessage> handler);
    List<FullMessage<PackageMessage>> fetchRange(long minOffset, long maxOffset) throws InterruptedException;
    QueueState getQueueState(String pubAgentName, String subAgentId);
    State getState(String pubAgentName, String subAgentId);
    Set<String> getSubscribedAgentIds(String pubAgentName);
}
