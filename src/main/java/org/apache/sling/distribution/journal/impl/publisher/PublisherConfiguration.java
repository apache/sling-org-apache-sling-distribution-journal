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
package org.apache.sling.distribution.journal.impl.publisher;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(name = "Apache Sling Journal based Distribution - Pub Agent",
        description = "Apache Sling Content Distribution Pub agent")
public @interface PublisherConfiguration {

    @AttributeDefinition
    String webconsole_configurationFactory_nameHint() default "Agent name: {name}";

    @AttributeDefinition(name = "name", description = "The name of the agent")
    String name() default "";

    @AttributeDefinition(name = "DistributionPackageBuilder target",
            description = "The target reference for the DistributionPackageBuilder used to build/install content packages, e.g. use target=(name=...) to bind to a service by name.")
    String packageBuilder_target() default "(name=...)";

    @AttributeDefinition(name = "Package queued timeout",
            description = "Timeout in ms to be used when waiting for a package to be queued")
    int queuedTimeout() default 60000;

}
