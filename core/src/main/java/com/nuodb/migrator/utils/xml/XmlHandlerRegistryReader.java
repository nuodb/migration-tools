/**
 * Copyright (c) 2015, NuoDB, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of NuoDB, Inc. nor the names of its contributors may
 *       be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL NUODB, INC. BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.utils.xml;

import com.nuodb.migrator.utils.Priority;
import com.nuodb.migrator.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.nuodb.migrator.utils.ReflectionUtils.getClassLoader;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.String.format;

public class XmlHandlerRegistryReader {

    protected transient final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final List<URL> resources = newArrayList();

    public void addRegistry(String resource) {
        addRegistry(getClassLoader().getResource(resource));
    }

    public void addRegistry(URL resource) {
        isNotNull(resource, "Handler registry resource is required");
        resources.add(resource);
    }

    public void read(XmlHandlerRegistry registry) {
        for (URL resource : resources) {
            try {
                read(registry, resource.openStream());
            } catch (IOException e) {
                throw new XmlPersisterException(format("Failed reading handler registry %1$s", resource));
            }
        }
    }

    protected void read(XmlHandlerRegistry registry, InputStream input) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String value;
        try {
            while ((value = reader.readLine()) != null) {
                read(registry, value);
            }
        } catch (IOException e) {
            throw new XmlPersisterException("Failed loading registry", e);
        } finally {
            try {
                input.close();
            } catch (IOException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Failed closing input stream", e);
                }
            }
        }
    }

    protected void read(XmlHandlerRegistry registry, String value) {
        value = value.trim();
        int comma = value.lastIndexOf(",");
        String handlerClassAsText;
        int priority = Priority.NORMAL;
        if (comma == -1) {
            handlerClassAsText = value;
        } else {
            handlerClassAsText = value.substring(0, comma);
            String priorityAsText = value.substring(comma + 1).trim();
            if (priorityAsText.length() != 0) {
                priority = Integer.parseInt(priorityAsText);
            }
        }
        XmlHandler handler = ReflectionUtils.newInstance(handlerClassAsText);
        registry.registerHandler(handler, priority);
    }
}
