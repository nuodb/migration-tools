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
package com.nuodb.migrator.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collection;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Thread.currentThread;

/**
 * @author Sergey Bushik
 */
public class ConfigUtils {

    private static final String PREFIX = "#";

    /**
     * Opens input stream to the specified path, which can be a class resources
     * resolved using the context class loader, a url or a file
     *
     * @param path
     *            path to the config, which is a class resources or a url or a
     *            file
     * @return input stream or null if the path wasn't recognized
     */
    public static InputStream loadConfig(String path) {
        return loadConfig(path, currentThread().getContextClassLoader());
    }

    /**
     * Opens input stream to the specified path, which can be a class resources
     * resolved using the provided class loader, a url or a file
     *
     * @param path
     *            path to the config, which is a class resources or a url or a
     *            file
     * @return input stream or null if the path wasn't recognized
     */
    public static InputStream loadConfig(String path, ClassLoader classLoader) {
        InputStream input = null;
        URL resource = classLoader.getResource(path);
        if (resource != null) {
            try {
                input = resource.openStream();
            } catch (IOException exception) {
                // it's not a class
            }
        }
        if (input == null) {
            try {
                input = new URL(path).openStream();
            } catch (IOException exception) {
                // it's not an url
            }
        }
        if (input == null) {
            try {
                input = new FileInputStream(path);
            } catch (IOException exception) {
                // it's not a file
            }
        }
        return input;
    }

    /**
     * Reads config line by line, ignores lines starting with comment symbol #
     *
     * @param input
     *            to read config lines from
     * @return a list of parameters
     * @throws IOException
     *             if any input/output error during reading input occurs
     */
    public static Collection<String> parseConfig(InputStream input) throws IOException {
        Collection<String> parameters = newArrayList();
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.length() != 0 && !line.startsWith(PREFIX)) {
                parameters.add(line);
            }
        }
        return parameters;
    }
}
