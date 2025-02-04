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
package org.apache.sling.distribution.journal.impl.eds;

import org.apache.sling.distribution.journal.bookkeeper.ContentPackageExtractor;
import org.apache.sling.distribution.journal.bookkeeper.PackageHandler;
import org.apache.sling.distribution.journal.bookkeeper.PackageHandlerFactory;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.osgi.service.component.annotations.Component;

@Component(service = PackageHandlerFactory.class, name = "edge-delivery")
public class EdsPackageHandlerFactory implements PackageHandlerFactory {
    @Override
    public PackageHandler create(DistributionPackageBuilder packageBuilder, ContentPackageExtractor extractor) {
        PackageHandler packageHandler = new EdsPackageHandler();
        return packageHandler;
    }
}
