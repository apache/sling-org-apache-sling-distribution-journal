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
package org.apache.sling.distribution.journal.impl.subscriber;

import org.apache.sling.commons.metrics.Gauge;
import org.apache.sling.distribution.journal.impl.queue.impl.PackageRetries;
import org.apache.sling.distribution.journal.impl.shared.DistributionMetricsService;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;

@Component(property = { 
        Constants.SERVICE_DESCRIPTION + "=Journal Availablility Status",
        Constants.SERVICE_VENDOR + "=The Apache Software Foundation",
        "name=" + DistributionMetricsService.SUB_COMPONENT + ".current_retries"})
public class CurrentRetries implements Gauge<Integer>{
    private PackageRetries packageRetries;
    
    public CurrentRetries() {
        this.packageRetries = new PackageRetries();
    }

    @Override
    public Integer getValue() {
        return this.packageRetries.getSum();
    }

    public PackageRetries getPackageRetries() {
        return this.packageRetries;
    }

}
