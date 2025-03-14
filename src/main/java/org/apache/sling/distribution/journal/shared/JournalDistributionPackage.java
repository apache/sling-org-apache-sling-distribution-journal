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

import org.apache.sling.distribution.packaging.DistributionPackage;
import org.apache.sling.distribution.packaging.DistributionPackageInfo;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class JournalDistributionPackage implements DistributionPackage {

    private final String id;

    private final String type;

    private final long size;

    private final byte[] data;

    private final DistributionPackageInfo info;

    public JournalDistributionPackage(@Nonnull String id, @Nonnull String type, @Nonnull byte[] data, @Nonnull DistributionPackageInfo info) {
        this.id = id;
        this.type = type;
        this.size = data.length;
        this.info = info;
        this.data = data;
    }

    @Nonnull
    @Override
    public String getId() {
        return id;
    }

    @Nonnull
    @Override
    public String getType() {
        return type;
    }

    @Override
    public @Nonnull InputStream createInputStream() {
        return new ByteArrayInputStream(data);
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void close() {
        // no resource to close
    }

    @Override
    public void delete() {
        // the journal is immutable with time based eviction policy
    }

    @Override
    public @Nonnull DistributionPackageInfo getInfo() {
        return info;
    }
}