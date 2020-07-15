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
package org.apache.sling.distribution.journal.impl.queue.impl;

import org.apache.sling.distribution.journal.impl.queue.CacheCallback;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProvider;
import org.apache.sling.distribution.journal.impl.queue.PubQueueProviderFactory;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.event.EventAdmin;

@Component
public class PubQueueProviderFactoryImpl implements PubQueueProviderFactory {
    
    @Reference
    private EventAdmin eventAdmin;
    
    private BundleContext context;

    public void activate(BundleContext context) {
        this.context = context;
    }

    @Override
    public PubQueueProvider create(CacheCallback callback) {
        return new PubQueueProviderImpl(eventAdmin, callback, context);
    }
    
}
