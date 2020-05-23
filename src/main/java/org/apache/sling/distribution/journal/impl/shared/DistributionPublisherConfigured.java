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

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.cm.ConfigurationEvent;
import org.osgi.service.cm.ConfigurationListener;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(immediate = true, service = ConfigurationListener.class)
public class DistributionPublisherConfigured implements ConfigurationListener {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionPublisherConfigured.class);

    private volatile BundleContext context;

    private ServiceRegistration<DistributionPublisherConfigured> reg;

    private final Object lock = new Object();

    @Activate
    public void activate(BundleContext context) {
        this.context = context;
    }

    @Deactivate
    public void deactivate() {
        synchronized (lock) {
            if (reg != null) {
                reg.unregister();
                LOG.info("Unregistered marker service");
            }
        }
    }

    @Override
    public void configurationEvent(ConfigurationEvent event) {
        if ("org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory".equals(event.getFactoryPid())
                && event.getType() != ConfigurationEvent.CM_DELETED) {
            synchronized (lock) {
                if (reg == null) {
                    reg = context.registerService(DistributionPublisherConfigured.class, this, null);
                    LOG.info("Registered marker service");
                }
            }
        }
    }
}
