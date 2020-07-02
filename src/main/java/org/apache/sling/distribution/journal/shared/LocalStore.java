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

import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ModifiableValueMap;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.api.resource.ResourceUtil;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.api.wrappers.ValueMapDecorator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;
import static org.apache.sling.api.resource.ResourceResolverFactory.SUBSERVICE;

@ParametersAreNonnullByDefault
public class LocalStore {

    private static final String ROOT_PATH = "/var/sling/distribution/journal/stores";

    private static final Logger LOG = LoggerFactory.getLogger(LocalStore.class);

    private final ResourceResolverFactory resolverFactory;

    private final String storeId;

    private final String rootPath;

    public LocalStore(ResourceResolverFactory resolverFactory, String storeType,
                       String storeId) {
        this.resolverFactory = Objects.requireNonNull(resolverFactory);
        this.rootPath = String.format("%s/%s", ROOT_PATH, Objects.requireNonNull(storeType));
        this.storeId = Objects.requireNonNull(storeId);
        createParent();
    }

    public synchronized void store(String key, Object value)
            throws PersistenceException {
        try (ResourceResolver resolver = requireNonNull(getBookKeeperServiceResolver())) {
            store(resolver, key, value);
            resolver.commit();
        } catch (LoginException e) {
            throw new RuntimeException("Failed to load data from the repository." + e.getMessage(), e);
        }
    }

    public synchronized void store(ResourceResolver serviceResolver, String key, Object value)
            throws PersistenceException {
        store(serviceResolver, Collections.singletonMap(key, value));
    }

    public synchronized void store(ResourceResolver serviceResolver, Map<String, Object> map) throws PersistenceException {
        Resource parent = getParent(serviceResolver);
        Resource store = parent.getChild(storeId);

        if (store != null) {
            store.adaptTo(ModifiableValueMap.class).putAll(map);
        } else {
            serviceResolver.create(parent, storeId, map);
        }
        LOG.debug(String.format("Stored data %s for storeId %s", map.toString(), storeId));
    }

    public <T> T load(String key, Class<T> clazz) {
        return load().get(key, clazz);
    }

    public <T> T load(String key, T defaultValue) {
        LOG.debug(String.format("Loading key %s for storeId %s with default value %s", key, storeId, defaultValue));
        return load().get(key, defaultValue);
    }

    public ValueMap load() {
        LOG.debug(String.format("Loading data for storeId %s", storeId));
        try (ResourceResolver serviceResolver = requireNonNull(getBookKeeperServiceResolver())) {
            Resource parent = getParent(serviceResolver);
            Resource store = parent.getChild(storeId);
            Map<String, Object> properties = (store != null) ? filterJcrProperties(store.getValueMap()) : emptyMap();
            LOG.debug(String.format("Loaded data %s for storeId %s", properties.toString(), storeId));
            return new ValueMapDecorator(properties);
        } catch (LoginException e) {
            throw new RuntimeException("Failed to load data from the repository." + e.getMessage(), e);
        }
    }

    @Nonnull
    private Resource getParent(ResourceResolver resolver) {
        String msg = "Parent path " + rootPath + " must be created during provisioning.";
        return requireNonNull(resolver.getResource(rootPath), msg);
    }

    private void createParent() {
        try (ResourceResolver resolver = getBookKeeperServiceResolver()) {
            ResourceUtil.getOrCreateResource(resolver,
                    rootPath, "sling:Folder", "sling:Folder", true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create parent path " + rootPath + ". " + e.getMessage(), e);
        }
    }

    private Map<String, Object> filterJcrProperties(ValueMap map) {
        return map.entrySet().stream()
                .filter(e -> ! e.getKey().startsWith("jcr:"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private ResourceResolver getBookKeeperServiceResolver() throws LoginException {
        return resolverFactory.getServiceResourceResolver(singletonMap(SUBSERVICE, "bookkeeper"));
    }
}
