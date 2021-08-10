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

import java.io.Closeable;
import java.util.Dictionary;
import java.util.Hashtable;

import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class SubscriberIdleCheck implements HealthCheck, Closeable {
    static final String CHECK_NAME = "DistributionSubscriber idle";
    private final ServiceRegistration<HealthCheck> reg;
    private final IdleCheck idleCheck;

    public SubscriberIdleCheck(BundleContext context, IdleCheck idleCheck) {
        this.idleCheck = idleCheck;
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(HealthCheck.NAME, CHECK_NAME);
        this.reg = context.registerService(HealthCheck.class, this, props);
    }

    @Override
    public Result execute() {
        Result.Status status = idleCheck.isIdle() ? Result.Status.OK : Result.Status.TEMPORARILY_UNAVAILABLE;
        return new Result(status, CHECK_NAME);
    }

    @Override
    public void close() {
        if (reg != null) {
            reg.unregister();
        }
    }
}
