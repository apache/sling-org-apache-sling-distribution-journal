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
package org.apache.sling.distribution.journal.bookkeeper;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.vault.fs.io.ImportOptions;
import org.apache.jackrabbit.vault.packaging.JcrPackage;
import org.apache.jackrabbit.vault.packaging.JcrPackageManager;
import org.apache.jackrabbit.vault.packaging.PackageException;
import org.apache.jackrabbit.vault.packaging.Packaging;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.distribution.common.DistributionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hook that can be added to a DistributionPackageBuilder.
 * Each distribution package is inspected for possible content packages in /etc/packages.
 * Such content packages are installed via the Packaging service.
 */
class ContentPackageExtractor {
    private static final String PACKAGE_BASE_PATH = "/etc/packages/";

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Packaging packageService;
    private final PackageHandling packageHandling;
    
    public ContentPackageExtractor(Packaging packageService, PackageHandling packageHandling) {
        this.packageService = packageService;
        this.packageHandling = packageHandling;
    }
    
    public void handle(ResourceResolver resourceResolver, List<String> paths) throws DistributionException {
        requireNonNull(resourceResolver, "Must provide resourceResolver");
        if (packageHandling == PackageHandling.Off) {
            return;
        }
        log.debug("Scanning imported nodes for packages to install.");
        for (String path : paths) {
            try {
                Resource resource = resourceResolver.getResource(path);
                if (resource != null) {
                    Node node = resource.adaptTo(Node.class);
                    if (isContentPackage(path, node)) {
                        installPackage(path, node);
                    }
                } else {
                    log.warn("Imported node {} does not exist. Skipping.", path);
                }
            } catch (Exception e) {
                throw new DistributionException("Error trying to extract package at path " + path, e);
            }
        }
    }

    private boolean isContentPackage(String path, Node node) throws RepositoryException {
        return path.startsWith(PACKAGE_BASE_PATH) && node.isNodeType(NodeType.NT_FILE);
    }

    private void installPackage(String path, Node node) throws RepositoryException, PackageException, IOException {
        log.info("Content package received at {}. Starting import.\n", path);
        Session session = node.getSession();
        JcrPackageManager packMgr = packageService.getPackageManager(session);
        try (JcrPackage pack = packMgr.open(node)) {
            if (pack != null) {
                installPackage(pack);
            }
        }
    }

    private void installPackage(JcrPackage pack) throws RepositoryException, PackageException, IOException {
        ImportOptions opts = new ImportOptions();
        if (packageHandling == PackageHandling.Extract) {
            pack.extract(opts);
        } else {
            pack.install(opts);
        }
    }

}
