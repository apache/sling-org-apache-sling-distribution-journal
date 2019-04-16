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
package org.apache.sling.distribution.journal.impl.subscriber;

import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.sling.distribution.journal.messages.Messages.PackageStatusMessage.Status;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.commons.io.IOUtils;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a precondition that watches status messages from other instances in order to confirm that a package can be processed.
 * The check will block until a status is found. If no status is received in 60 seconds it will throw an exception.
 */
@Component(immediate = true, service = Precondition.class, property = { "name=staging" },
        configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(ocd = StagingPrecondition.Configuration.class)
public class StagingPrecondition implements Precondition {

    private static final Logger LOG = LoggerFactory.getLogger(StagingPrecondition.class);


    @Reference
    private MessagingProvider messagingProvider;

    @Reference
    private Topics topics;

    private volatile PackageStatusWatcher watcher;

    private volatile boolean running = true;


    @Activate
    public void activate(Configuration config) {
        String subAgentName = config.subAgentName();
        watcher = new PackageStatusWatcher(messagingProvider, topics, subAgentName);
        LOG.info("Activated Staging Precondition for subAgentName {}", subAgentName);
    }

    @Deactivate
    public void deactivate() {
        IOUtils.closeQuietly(watcher);
        running = false;
    }


    @Override
    public boolean canProcess(long pkgOffset, int timeoutSeconds) {

        if (timeoutSeconds < 1) {
            throw new IllegalArgumentException();
        }

        // clear all offsets less than the required one as they are not needed anymore.
        // this works OK only if pkgOffset are always queried in increasing order.
        watcher.clear(pkgOffset);

        // try to get the status for timeoutSeconds and then throw
        for(int i=0; running && i < timeoutSeconds; i++) {
            Status status = watcher.getStatus(pkgOffset);

            if (status != null) {
                return status == Status.IMPORTED;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
            }
        }

        throw new IllegalStateException("Timeout waiting for package offset " + pkgOffset + " on status topic.");
    }


    @ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Staged Distribution Precondition",
            description = "Apache Sling Content Distribution Sub Agent precondition for staged replication")
    public @interface Configuration {

        @AttributeDefinition
        String webconsole_configurationFactory_nameHint() default "Agent name: {subAgentName}";

        @AttributeDefinition(name = "name", description = "The name of the agent to watch")
        String subAgentName() default "";
    }
}
