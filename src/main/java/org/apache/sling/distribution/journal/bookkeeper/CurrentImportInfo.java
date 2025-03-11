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

import java.time.Duration;

import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CurrentImportInfo {
	private final Logger log = LoggerFactory.getLogger(BookKeeper.class);
	public static final Duration IMPORT_TIME_WARN_LEVEL = Duration.ofMinutes(5);

	private final PackageMessage pkgMsg;
	private final long offset;
	private final long startTime;

	public CurrentImportInfo(PackageMessage pkgMsg, long offset, long importStartTime) {
		this.pkgMsg = pkgMsg;
		this.offset = offset;
		this.startTime = importStartTime;
	}

	public PackageMessage getPkgMsg() {
		return pkgMsg;
	}

	public long getOffset() {
		return offset;
	}

	public long getImportStartTime() {
		return startTime;
	}

    Long getCurrentImportDuration() {
        long currentImportDurationMs = System.currentTimeMillis() - startTime;
        if (currentImportDurationMs > IMPORT_TIME_WARN_LEVEL.toMillis()) {
            log.warn("Import of package={}, offset={} takes currentImportTimeSeconds={} which is longer than warnLevelSeconds={}", 
            		pkgMsg, offset, currentImportDurationMs / 1000, IMPORT_TIME_WARN_LEVEL.toSeconds());
        }
        return currentImportDurationMs;
    }
}
