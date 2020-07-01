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

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

/**
 * Manages the binary content of DistributionPackages. If they are too big to fit in a journal message then they
 * are written to the blob store. It also offers cleanup functionality to remove the data when it is not needed anymore.
 */
@Component(service = PackageRepo.class)
@ParametersAreNonnullByDefault
public class PackageRepo {

    private static final String SLING_FOLDER = "sling:Folder";

    @Reference
    private ResourceResolverFactory resolverFactory;
    
    @Reference
    private ServiceUserMapped mapped;

    @Reference
    private DistributionMetricsService distributionMetricsService;

    private static final Logger LOG = LoggerFactory.getLogger(PackageRepo.class);
    static final String PACKAGES_ROOT_PATH = "/var/sling/distribution/journal/packages";
    private static final String PACKAGE_PATH_PATTERN = PACKAGES_ROOT_PATH + "/%s/data/%s"; // packageType x packageId


    @Nonnull
    public String store(ResourceResolver resolver, DistributionPackage disPkg)
            throws DistributionException {
        try {
            String pkgPath = String.format(PACKAGE_PATH_PATTERN, disPkg.getType(), disPkg.getId());
            Resource pkgResource = ResourceUtil.getOrCreateResource(resolver,
                    pkgPath, SLING_FOLDER, SLING_FOLDER, false);
            Node pkgNode = pkgResource.adaptTo(Node.class);
            Node binNode = JcrUtils.getOrAddNode(pkgNode, "bin", NodeType.NT_FILE);
            Node cntNode = JcrUtils.getOrAddNode(binNode, Node.JCR_CONTENT, NodeType.NT_RESOURCE);
            Binary binary = pkgNode.getSession().getValueFactory().createBinary(disPkg.createInputStream());
            cntNode.setProperty(Property.JCR_DATA, binary);
            resolver.commit();
            String blobRef = ((ReferenceBinary) binary).getReference();
            LOG.info("Stored content package {} under path {} with blobRef {}",
                    disPkg.getId(), pkgPath, blobRef);
            return blobRef;
        } catch (Exception e) {
            throw new DistributionException(e.getMessage(), e);
        }
    }

    /**
     * Delete all packages that are older than specified unix time
     * @param deleteOlderThanTime 
     */
    public void cleanup(long deleteOlderThanTime) {
        Timer.Context context = distributionMetricsService.getCleanupPackageDuration().time();
        // Auto-refresh policy is disabled for service resource resolver
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"))) {
            
            PackageCleaner packageCleaner = new PackageCleaner(resolver, deleteOlderThanTime);
            Resource root = getRoot(resolver);
            int removedCount = packageCleaner.cleanup(root);
            distributionMetricsService.getCleanupPackageRemovedCount().increment(removedCount);
        } catch (LoginException | PersistenceException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            context.stop();
        }
    }

    @Nonnull
    private Resource getRoot(ResourceResolver resolver)
            throws PersistenceException {
        return ResourceUtil.getOrCreateResource(resolver, PACKAGES_ROOT_PATH, SLING_FOLDER, SLING_FOLDER, true);
    }
}
