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

import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PackageCleaner {
    private static final Logger LOG = LoggerFactory.getLogger(PackageRepo.class);
    private ResourceResolver resolver;
    private long deleteOlderThanTime;

    /**
     * Delete all packages older than specified time
     * 
     * @param resolver
     * @param deleteOlderThanTime
     */
    public PackageCleaner(ResourceResolver resolver, long deleteOlderThanTime) {
        this.resolver = resolver;
        this.deleteOlderThanTime = deleteOlderThanTime;
    }
    
    public int cleanup(Resource root)
            throws PersistenceException {
        int removedCount = 0;
        for (Resource type : root.getChildren()) {
            Resource data = type.getChild("data");
            if (data != null) {
                for (Resource pkgNode : data.getChildren()) {
                    removedCount += cleanNode(pkgNode);
                }
            }
        }
        if (resolver.hasChanges()) {
            resolver.commit();
        }
        return removedCount;
    }
    
    private int cleanNode(Resource pkgNode)
            throws PersistenceException {
        long createdTime  = pkgNode.getValueMap().get("jcr:created", Long.class);
        if (createdTime < deleteOlderThanTime) {
            LOG.info("removing package={}, created={} < deleteTime={}", pkgNode.getName(), createdTime, deleteOlderThanTime);
            resolver.delete(pkgNode);
            return 1;
        } else {
            return 0;
        }
    }

}
