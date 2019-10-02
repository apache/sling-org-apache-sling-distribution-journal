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
package org.apache.sling.distribution.journal.impl.shared;

import java.time.Duration;
import java.util.List;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.JournalAvailable;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.impl.queue.impl.LimitPoller;
import org.apache.sling.distribution.journal.messages.Messages.PackageMessage;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

@Component(service = PackageBrowser.class)
public class PackageBrowser {
    
    @Reference
    private JournalAvailable journalAvailable;
    
    @Reference
    private MessagingProvider messagingProvider;
    
    @Reference
    private Topics topics;
    
    public List<FullMessage<PackageMessage>> getMessages(long startOffset, long numMessages, Duration timeout) {
        LimitPoller poller = new LimitPoller(messagingProvider, topics.getPackageTopic(), startOffset, numMessages);
        return poller.fetch(timeout);
    }
}
