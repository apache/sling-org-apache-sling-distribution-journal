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

import org.apache.sling.distribution.ImportPreProcessor;
import org.osgi.service.component.annotations.Component;

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * A no-operation (no-op) implementation of the {@link ImportPreProcessor} interface.
 * This class is intended as a placeholder or default implementation that performs
 * no actions when its {@link #process(Map)} method is invoked. It's useful in contexts
 * where an {@link ImportPreProcessor} is required but no pre-import processing is necessary.
 */
@Component(property = { "type=default" })
public class NoOpImportPreProcessor implements ImportPreProcessor {

    /**
     * Does not perform any pre-import processing on the given properties. This method
     * is intentionally left empty, making this class a no-operation implementation.
     *
     * @param props properties defining the content to be imported; not used by this method.
     */
    @Override
    public void process(@Nonnull Map<String, Object> props) {}
}
