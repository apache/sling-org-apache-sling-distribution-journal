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
package org.apache.sling.distribution.journal.shared;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = Topics.class)
@Designate(ocd = Topics.TopicsConfiguration.class)
public class Topics {

    private static final Logger LOG = LoggerFactory.getLogger(Topics.class);

    public static final String PACKAGE_TOPIC = "aemdistribution_package";
    public static final String DISCOVERY_TOPIC = "aemdistribution_discovery";
    public static final String STATUS_TOPIC = "aemdistribution_status";
    public static final String COMMAND_TOPIC = "aemdistribution_command";
    public static final String EVENT_TOPIC = "aemdistribution_event";
    
    private String discoveryTopic;
    private String packageTopic;
    private String statusTopic;
    private String commandTopic;
    private String eventTopic;
    
    public Topics() {
        packageTopic = PACKAGE_TOPIC;
        discoveryTopic = DISCOVERY_TOPIC;
        statusTopic = STATUS_TOPIC;
        commandTopic = COMMAND_TOPIC;
        eventTopic = EVENT_TOPIC;
    }

    
    @Activate
    public void activate(TopicsConfiguration config) {
        this.packageTopic = config.packageTopic();
        this.discoveryTopic = config.discoveryTopic();
        this.statusTopic = config.statusTopic();
        this.commandTopic = config.commandTopic();
        this.eventTopic = config.eventTopic();
        LOG.info("Topics service started with packageTopic '{}' discoveryTopic '{}' statusTopic '{}' eventTopic '{}' commandTopic '{}'",
                packageTopic, discoveryTopic, statusTopic, eventTopic, commandTopic);
    }
    
    public String getPackageTopic() {
        return packageTopic;
    }
    
    public String getDiscoveryTopic() {
        return discoveryTopic;
    }

    public String getStatusTopic() {
        return statusTopic;
    }

    public String getCommandTopic() {
        return commandTopic;
    }

    public String getEventTopic() {
        return eventTopic;
    }

    
    @ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Topics")
    public @interface TopicsConfiguration {

        @AttributeDefinition(name = "Packages Topic",
                description = "The topic for package messages.")
        String packageTopic() default PACKAGE_TOPIC;

        @AttributeDefinition(name = "Discovery Topic",
                description = "The topic for discovery messages.")
        String discoveryTopic() default DISCOVERY_TOPIC;

        @AttributeDefinition(name = "Status Topic",
                description = "The topic for status messages.")
        String statusTopic() default STATUS_TOPIC;

        @AttributeDefinition(name = "Command Topic",
                description = "The topic for command messages.")
        String commandTopic() default COMMAND_TOPIC;

        @AttributeDefinition(name = "Event Topic",
                description = "The optional topic for event messages. If the topic is blank, no event message is sent.")
        String eventTopic() default EVENT_TOPIC;

    }

}
