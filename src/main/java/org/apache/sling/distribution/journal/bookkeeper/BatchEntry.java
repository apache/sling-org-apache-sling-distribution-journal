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

import java.util.Date;

import org.apache.sling.distribution.journal.messages.PackageMessage;

import static java.util.Objects.requireNonNull;

/**
 * A package message together with its journal offset, as part of a batch import.
 */
public final class BatchEntry {
    private final PackageMessage message;
    private final long offset;
    private final Date createdTime;

    public BatchEntry(PackageMessage message, long offset, Date createdTime) {
        this.message = requireNonNull(message);
        this.offset = offset;
        this.createdTime = requireNonNull(createdTime);
    }

    public PackageMessage getMessage() {
        return message;
    }

    public long getOffset() {
        return offset;
    }

    public Date getCreatedTime() {
        return createdTime;
    }
}
