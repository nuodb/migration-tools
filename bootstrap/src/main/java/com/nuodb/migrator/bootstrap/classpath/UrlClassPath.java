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
package com.nuodb.migrator.bootstrap.classpath;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * @author Sergey Bushik
 */
public class UrlClassPath implements ClassPath {

    private Collection<URL> urls = new LinkedHashSet<URL>();

    public UrlClassPath(String url) {
        try {
            urls.add(new URL(url));
        } catch (MalformedURLException exception) {
            throw new ClassPathException(exception);
        }
    }

    public UrlClassPath(Collection<URL> urls) {
        this.urls = urls;
    }

    @Override
    public void exposeClassPath(ClassPathLoader classPathLoader) {
        for (URL url : urls) {
            classPathLoader.addUrl(url);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof UrlClassPath))
            return false;

        UrlClassPath that = (UrlClassPath) o;

        if (urls != null ? !urls.equals(that.urls) : that.urls != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return urls != null ? urls.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "UrlClassPath{urls=" + urls + '}';
    }
}
