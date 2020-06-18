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
package org.apache.sling.distribution.journal.impl.shared;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.sling.distribution.journal.FullMessage;
import org.apache.sling.distribution.journal.MessageInfo;
import org.apache.sling.distribution.journal.MessagingProvider;
import org.apache.sling.distribution.journal.messages.PackageMessage;
import org.apache.sling.distribution.journal.messages.PackageMessage.ReqType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PackageViewerPluginTest {

    @Spy
    Topics topics = new Topics();

    @Mock
    private HttpServletRequest req;
    
    @Mock
    private HttpServletResponse res;
    
    @Mock
    MessagingProvider messagingProvider;
    
    @Mock
    ServletOutputStream servletOutputStream;
    
    @Mock
    PackageBrowser packageBrowser;
    
    @InjectMocks
    PackageViewerPlugin viewer;

    private StringWriter outWriter;

    @Before
    public void before() throws IOException {
        FullMessage<PackageMessage> msg1 = createPackageMsg(1L);
        List<FullMessage<PackageMessage>> messages  = Collections.singletonList(msg1);
        doReturn(messages).when(packageBrowser).getMessages(Mockito.eq(1L), Mockito.anyLong(), Mockito.any());
        doReturn(messages).when(packageBrowser).getMessages(Mockito.eq(0L), Mockito.anyLong(), Mockito.any());
        doReturn(emptyList()).when(packageBrowser).getMessages(Mockito.eq(2L), Mockito.anyLong(), Mockito.any());

        outWriter = new StringWriter();
        when(res.getWriter()).thenReturn(new PrintWriter(outWriter));
        when(res.getOutputStream()).thenReturn(servletOutputStream);
    }

    @Test
    public void testSimple() {
        assertThat(viewer.getLabel(), equalTo("distpackages"));
        assertThat(viewer.getTitle(), equalTo("Distribution Package Viewer"));
        assertThat(viewer.getCategory(), equalTo("Sling"));
    }
    
    @Test
    public void testPackageList() throws ServletException, IOException {
        when(req.getPathInfo()).thenReturn("/distpackages");
        
        viewer.renderContent(req, res);

        String outString = outWriter.getBuffer().toString();
        System.out.println(outString);
        assertThat(outString, 
                containsString("<tr><td><a href=\"distpackages/1\">pkgid</a></td><td>1</td><td>ADD</td><td>[/content]</td></tr>"));
    }

    @Test
    public void testGetPackage() throws ServletException, IOException {
        when(req.getPathInfo()).thenReturn("/distpackages/1");
        
        viewer.renderContent(req, res);
        
        verify(packageBrowser).getMessages(Mockito.eq(1L), Mockito.eq(1L), Mockito.any());
    }
    
    @Test
    public void testGetPackageNotFound() throws ServletException, IOException {
        when(req.getPathInfo()).thenReturn("/distpackages/2");
        
        viewer.renderContent(req, res);
        
        verify(res).setStatus(Mockito.eq(404));
        verify(packageBrowser).getMessages(Mockito.eq(2L), Mockito.eq(1L), Mockito.any());
    }
    
    @Test
    public void testIsHtmlPackage() throws ServletException, IOException {
        when(req.getPathInfo()).thenReturn("/distpackages/1");

        assertThat(viewer.isHtmlRequest(req), equalTo(false));
    }

    @Test
    public void testIsHtmlMain() throws ServletException, IOException {
        when(req.getPathInfo()).thenReturn("/distpackages");

        assertThat(viewer.isHtmlRequest(req), equalTo(true));
    }

    private FullMessage<PackageMessage> createPackageMsg(long offset) {
        MessageInfo info = new TestMessageInfo("topic", 0 , offset, 0L);
        PackageMessage message = PackageMessage.builder()
                .pubSlingId("")
                .reqType(ReqType.ADD)
                .paths(Arrays.asList("/content"))
                .pkgId("pkgid")
                .pkgType("some_type")
                .pkgBinary("package content".getBytes(Charset.defaultCharset()))
                .build();
        return new FullMessage<>(info, message);
    }

}
