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
package org.apache.sling.distribution.journal.impl.queue;

import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_OFFSET;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_PARTITION;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TIMESTAMP;
import static org.apache.sling.distribution.journal.impl.queue.QueueItemFactory.RECORD_TOPIC;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_PACKAGE_TYPE;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_DEEP_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_PATHS;
import static org.apache.sling.distribution.packaging.DistributionPackageInfo.PROPERTY_REQUEST_TYPE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertThat;

import java.util.Arrays;

import org.apache.sling.distribution.DistributionRequestType;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.apache.sling.distribution.journal.shared.TestMessageInfo;
import org.apache.sling.distribution.queue.DistributionQueueItem;
import org.hamcrest.Matcher;
import org.junit.Test;

public class QueueItemFactoryTest {

	private DistributionQueueItem item;

	@Test
	public void test() {
		MessageInfo info = new TestMessageInfo("topic", 0, 1, 2);
		PackageMessage message = PackageMessage.builder()
				.pubSlingId("sling1")
				.pkgId("pkg1")
				.pkgType("type")
				.paths(Arrays.asList("/"))
				.deepPaths(Arrays.asList("/deep"))
				.reqType(ReqType.ADD)
				.build();
		
		item = QueueItemFactory.fromPackage(info, message, true);
		
		assertProp(RECORD_TOPIC, String.class, equalTo("topic"));
		assertProp(RECORD_PARTITION, Integer.class, equalTo(0));
		assertProp(RECORD_OFFSET, Long.class, equalTo(1L));
		assertProp(RECORD_TIMESTAMP, Long.class, equalTo(2L));
		assertProp(PROPERTY_PACKAGE_TYPE, String.class, equalTo("type"));
		assertProp(PROPERTY_REQUEST_TYPE, DistributionRequestType.class, equalTo(DistributionRequestType.ADD));
		assertProp(PROPERTY_REQUEST_PATHS, String[].class, hasItemInArray("/"));
		assertProp(PROPERTY_REQUEST_DEEP_PATHS, String[].class, hasItemInArray("/deep"));
		assertProp(QueueItemFactory.PACKAGE_MSG, PackageMessage.class, equalTo(message));
	}
	
	private <T> void assertProp(String key, Class<T> type, Matcher<T> matcher) {
		assertThat(item.get(key, type), matcher);
	}
}
