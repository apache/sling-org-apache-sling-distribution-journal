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

public class BookKeeperConfig {
    private final String subAgentName;
    private final String subSlingId;
    private final boolean editable;
    private final int maxRetries;
    private final PackageHandling packageHandling;

    public BookKeeperConfig(String subAgentName,
            String subSlingId,
            boolean editable, 
            int maxRetries,
            PackageHandling packageHandling) {
                this.subAgentName = subAgentName;
                this.subSlingId = subSlingId;
                this.editable = editable;
                this.maxRetries = maxRetries;
                this.packageHandling = packageHandling;
    }
    
    public String getSubAgentName() {
        return subAgentName;
    }
    
    public String getSubSlingId() {
        return subSlingId;
    }
    
    public boolean isEditable() {
        return editable;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public PackageHandling getPackageHandling() {
        return packageHandling;
    }
    
    @Override
    public String toString() {
        return String.format("subAgentName=%S, subSlingId=%s, editable=%s, maxRetries=%s, packageHandling=%s",
                subAgentName, subSlingId, editable, maxRetries, packageHandling);
    }
}
