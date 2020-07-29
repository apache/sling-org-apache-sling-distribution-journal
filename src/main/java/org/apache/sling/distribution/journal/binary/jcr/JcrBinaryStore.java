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
package org.apache.sling.distribution.journal.binary.jcr;

import static java.util.Collections.singletonMap;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.api.ReferenceBinary;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.commons.jackrabbit.SimpleReferenceBinary;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.commons.metrics.Timer;
import org.apache.sling.distribution.common.DistributionException;
import org.apache.sling.distribution.journal.BinaryStore;
import org.apache.sling.distribution.journal.shared.DistributionMetricsService;
import org.apache.sling.serviceusermapping.ServiceUserMapped;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(
    property = {
        "type=jcr"
    },
    configurationPolicy = ConfigurationPolicy.REQUIRE
)
public class JcrBinaryStore implements BinaryStore {
    private static final long MAX_INLINE_PKG_BINARY_SIZE = 800L * 1024;
    private static final String SLING_FOLDER = "sling:Folder";
    static final String PACKAGES_ROOT_PATH = "/var/sling/distribution/journal/packagebinaries";

    private static final Logger LOG = LoggerFactory.getLogger(JcrBinaryStore.class);

    @Reference
    private ServiceUserMapped mapped;

    @Reference
    private DistributionMetricsService distributionMetricsService;


    @Reference
    private ResourceResolverFactory resolverFactory;

    @Override public InputStream get(String reference) throws IOException {
        try (ResourceResolver resolver = createResourceResolver()) {
            Session session = resolver.adaptTo(Session.class);
            if (session == null) {
                throw new IOException("Unable to get Oak session");
            }
            ValueFactory factory = session.getValueFactory();
            Binary binary = factory.createValue(new SimpleReferenceBinary(reference)).getBinary();
            return binary.getStream();
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public String put(String id, InputStream stream, long length) throws IOException {
        if (length > MAX_INLINE_PKG_BINARY_SIZE) {

            /*
             * Rather than pro-actively (and somewhat arbitrarily)
             * decide to avoid sending a package inline based on
             * its size, we could simply try to send packages of
             * any size and only avoiding to inline as a fallback.
             * However, this approach requires the messaging
             * implementation to offer a mean to distinguish
             * size issues when sending messages, which is not
             * always the case.
             */

            LOG.info("Package {} too large ({}B) to be sent inline", id, length);
            try {
                return store(id, stream);
            } catch (DistributionException e) {
                throw new IOException(e.getMessage(), e);
            }
        }
        return null;
    }
    
    @Nonnull
    public String store(String id, InputStream binaryStream)throws DistributionException {
        try (ResourceResolver resolver = createResourceResolver()) {
            String pkgPath = PACKAGES_ROOT_PATH + "/" + id;
            Resource pkgResource = ResourceUtil.getOrCreateResource(resolver,
                    pkgPath, SLING_FOLDER, SLING_FOLDER, false);
            Node pkgNode = Objects.requireNonNull(pkgResource.adaptTo(Node.class));
            Node binNode = JcrUtils.getOrAddNode(pkgNode, "bin", NodeType.NT_FILE);
            Node cntNode = JcrUtils.getOrAddNode(binNode, Node.JCR_CONTENT, NodeType.NT_RESOURCE);
            Binary binary = pkgNode.getSession().getValueFactory().createBinary(binaryStream);
            cntNode.setProperty(Property.JCR_DATA, binary);
            resolver.commit();
            String blobRef = ((ReferenceBinary) binary).getReference();
            LOG.info("Stored content package {} under path {} with blobRef {}",
                    id, pkgPath, blobRef);
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
        try (ResourceResolver resolver = createResourceResolver()) {
            
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

    private ResourceResolver createResourceResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"));
    }
}
