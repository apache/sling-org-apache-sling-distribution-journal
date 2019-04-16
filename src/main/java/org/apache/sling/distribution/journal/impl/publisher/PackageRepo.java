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

import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.apache.sling.distribution.journal.impl.shared.Topics;
import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.Reset;

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

/**
 * Manages the binary content of DistributionPackages. If they are too big to fit in a journal message then they
 * are written to the blob store. It also offers cleanup functionality to remove the data when it is not needed anymore.
 */
@Component(service = PackageRepo.class)
@ParametersAreNonnullByDefault
public class PackageRepo {

    @Reference
    private ResourceResolverFactory resolverFactory;
    
    @Reference
    private Topics topics;
    
    @Reference
    private MessagingProvider messagingProvider;
    
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
                    pkgPath, "sling:Folder", "sling:Folder", false);
            Node pkgNode = pkgResource.adaptTo(Node.class);
            Node binNode = JcrUtils.getOrAddNode(pkgNode, "bin", NodeType.NT_FILE);
            Node cntNode = JcrUtils.getOrAddNode(binNode, Node.JCR_CONTENT, NodeType.NT_RESOURCE);
            Binary binary = pkgNode.getSession().getValueFactory().createBinary(disPkg.createInputStream());
            cntNode.setProperty(Property.JCR_DATA, binary);
            resolver.commit();
            String blobRef = ((ReferenceBinary) binary).getReference();
            LOG.info(String.format("Stored content package %s under path %s with blobRef %s",
                    disPkg.getId(), pkgPath, blobRef));
            return blobRef;
        } catch (Exception e) {
            throw new DistributionException(e.getMessage(), e);
        }
    }

    /**
     * The cleanup algorithm is based on the head and tail offsets
     * fetched from the package topic.
     * 
     * The implementation is robust and fits any topic retention policy setting,
     * without requiring any explicit configuration matching the
     * actual retention policy. It is also robust against potential
     * instance clock misconfiguration/synchronisation issues.
     * 
     * This comes at the expense of saving offsets in the repository.
     * It should be an acceptable cost, given that the vast majority
     * of packages are not saved in the repository on the first place
     * and thus don't need cleanup.
     *
     * The cleanup does this:
     *
     * * The packages with an offset smaller than the head
     *   offset can be removed because they are no longer referenced
     *   by the package topic. 
     * 
     * * The packages with no offset are set the tail offset. This relies
     *   on the session being shielded from new packages that might be created
     *   in parallel.
     */
    public void cleanup() {
        Timer.Context context = distributionMetricsService.getCleanupPackageDuration().time();
        // Auto-refresh policy is disabled for service resource resolver
        try (ResourceResolver resolver = resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"))) {
            long headOffset = messagingProvider.retrieveOffset(topics.getPackageTopic(), Reset.earliest);
            long tailOffset = messagingProvider.retrieveOffset(topics.getPackageTopic(), Reset.latest);
            cleanup(resolver, headOffset, tailOffset);
        } catch (LoginException | PersistenceException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            context.stop();
        }
    }

    private void cleanup(ResourceResolver resolver, long headOffset, long tailOffset)
            throws PersistenceException {
        LOG.info(String.format("Cleanup headOffset %s tailOffset %s", headOffset, tailOffset));
        Resource root = getRoot(resolver);
        int removedCount = 0;
        for (Resource type : root.getChildren()) {
            Resource data = type.getChild("data");
            if (data != null) {
                for (Resource pkg : data.getChildren()) {
                    removedCount += cleanNode(resolver, headOffset, tailOffset, pkg);
                }
            }
        }
        if (resolver.hasChanges()) {
            resolver.commit();
            distributionMetricsService.getCleanupPackageRemovedCount().increment(removedCount);
        }
    }

    private int cleanNode(ResourceResolver resolver, long headOffset, long tailOffset, Resource pkg)
            throws PersistenceException {
        long offset = pkg.getValueMap().get("offset", -1);
        if (offset < 0) {
            LOG.info(String.format("keep package %s, setting tail offset %s", pkg.getName(), tailOffset));
            pkg.adaptTo(ModifiableValueMap.class).put("offset", tailOffset);
        } else if (offset < headOffset) {
            LOG.info(String.format("remove package %s, offset smaller than head offset %s < %s", pkg.getName(), offset, headOffset));
            resolver.delete(pkg);
            return 1;
        } else {
            LOG.debug(String.format("keep package %s, offset bigger or equal to head offset %s >= %s", pkg.getName(), offset, headOffset));
        }
        return 0;
    }

    @Nonnull
    private Resource getRoot(ResourceResolver resolver)
            throws PersistenceException {
        return ResourceUtil.getOrCreateResource(resolver, PACKAGES_ROOT_PATH, "sling:Folder", "sling:Folder", true);
    }
}
