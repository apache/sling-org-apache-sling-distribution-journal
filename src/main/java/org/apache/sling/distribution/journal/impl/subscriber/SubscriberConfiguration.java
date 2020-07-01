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

import org.apache.sling.distribution.journal.bookkeeper.PackageHandling;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Sub Agent",
        description = "Apache Sling Content Distribution Sub agent")
public @interface SubscriberConfiguration {

    @AttributeDefinition
    String webconsole_configurationFactory_nameHint() default "Agent name: {name}";

    @AttributeDefinition(name = "name", description = "The name of the agent")
    String name() default "";

    @AttributeDefinition(name = "Agent Names",
            description = "The SCD agent names to subscribe to.")
    String[] agentNames() default {""};

    @AttributeDefinition(name = "DistributionPackageBuilder target",
            description = "The target reference for the DistributionPackageBuilder used to build/install content packages, e.g. use target=(name=...) to bind to a service by name.")
    String packageBuilder_target() default "(name=journal_filevault)";

    @AttributeDefinition(name = "Precondition target",
            description = "The target reference for the Precondition used to validate packages, e.g. use target=(name=...) to bind to a service by name.")
    String precondition_target() default "(name=default)";

    @AttributeDefinition(name = "editable", description = "True if the agent supports removal of items, false otherwise. Default is false.")
    boolean editable() default false;

    @AttributeDefinition(name = "maxRetries", description = "The max number of attempts to import a package before moving the package to an error queue. If set to a negative value, the number of attempts is infinite. Default is -1 (infinite attempts).")
    int maxRetries() default -1;

    @AttributeDefinition(name = "packageHandling", description = "Defines if content packages in /etc/packages should be processed (Extract, Install, Off).")
    PackageHandling packageHandling() default PackageHandling.Off;
    
    @AttributeDefinition(name = "subscriberIdleCheck", description = "Defines if we register a subscriber idle health check.")
    boolean subscriberIdleCheck() default false;
}
