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
package org.apache.sling.distribution.journal.spi;

import javax.annotation.Nonnull;

import java.util.List;

import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageBuilder;
import org.osgi.annotation.versioning.ConsumerType;

/**
 * A {@link DistributionPackageBuilder} that can install several packages in one call.
 *
 * <p>A journal subscriber configured with {@code maxBatchSize > 1} hands consecutive
 * ADD and DELETE packages to {@link #installPackages(ResourceResolver, List)} instead of
 * installing them one by one. This lets a builder whose install step is slow (for example
 * because it calls a remote system per package) process independent packages concurrently.</p>
 *
 * <p>Contract for implementations:</p>
 * <ul>
 *     <li>The packages are passed in journal order. After a successful call, the result must be
 *     the same as installing the packages one by one in that order. Packages may only be installed
 *     concurrently when that cannot change the result, for example when their paths do not overlap
 *     (a package deleting {@code /a} and a package updating {@code /a/b} do overlap).</li>
 *     <li>Changes made through the given resource resolver are committed by the subscriber after the
 *     call returns, together with the journal offset of the last package. Implementations must not
 *     commit the resolver themselves.</li>
 *     <li>If the call throws, the subscriber does not store the offset and installs each package of
 *     the batch again through {@link #installPackage(ResourceResolver, DistributionPackage)}, with the
 *     usual retry and error-queue handling. Packages of a failed batch can therefore be installed more
 *     than once, so installing a package must be idempotent.</li>
 * </ul>
 */
@ConsumerType
public interface BatchDistributionPackageBuilder extends DistributionPackageBuilder {

    /**
     * Install the given packages.
     *
     * @param resourceResolver the resource resolver used to install the packages
     * @param distributionPackages the packages to install, in journal order
     * @throws DistributionException if any package of the batch could not be installed
     */
    void installPackages(
            @Nonnull ResourceResolver resourceResolver, @Nonnull List<DistributionPackage> distributionPackages)
            throws DistributionException;
}
