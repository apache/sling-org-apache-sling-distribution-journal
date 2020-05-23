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

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.osgi.service.component.annotations.ConfigurationPolicy.REQUIRE;

@Component(
        immediate = true,
        service = DistributionPublisherConfigured.class,
        configurationPolicy = REQUIRE,
        configurationPid = "org.apache.sling.distribution.journal.impl.publisher.DistributionPublisherFactory"
)
public class DistributionPublisherConfigured {

    private static final Logger LOG = LoggerFactory.getLogger(DistributionPublisherConfigured.class);

    @Activate
    public void activate() {
        LOG.info("activated");
    }

    @Activate
    public void deactivate() {
        LOG.info("deactivated");
    }
}
