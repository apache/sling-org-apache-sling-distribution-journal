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
package org.apache.sling.distribution.journal.impl.event;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.osgi.service.event.Event;

public class DistributionFailureEvent {
    public static final String TOPIC_PACKAGE_FAILURE = "org/apache/sling/distribution/journal/PACKAGE_FAILURE";

    public static final String PROPERTY_PACKAGE_MESSAGE = "package.message";
    public static final String PROPERTY_OFFSET = "offset";
    public static final String PROPERTY_CREATED_DATE = "created.date";
    public static final String PROPERTY_IMPORT_DURATION = "import.duration";
    public static final String PROPERTY_NUM_RETRIES = "num.retries";
    public static final String PROPERTY_WILL_DISCARD = "will.discard";
    public static final String PROPERTY_EXCEPTION = "exception";

    public static Event build(PackageMessage packageMessage, long offset, Date createdDate, int numRetries, 
            boolean willDiscard, Exception ex) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(PROPERTY_PACKAGE_MESSAGE, packageMessage);
        properties.put(PROPERTY_OFFSET, offset);
        properties.put(PROPERTY_CREATED_DATE, createdDate);
        properties.put(PROPERTY_NUM_RETRIES, numRetries);
        properties.put(PROPERTY_WILL_DISCARD, willDiscard);
        properties.put(PROPERTY_EXCEPTION, ex);
        
        return new Event(TOPIC_PACKAGE_FAILURE, properties);
    }
} 