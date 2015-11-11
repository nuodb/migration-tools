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
package com.nuodb.migrator.config;

import java.util.Properties;

/**
 * @author Sergey Bushik
 */
public abstract class Config {

    public static final String HOME = "nuodb.migrator.home";

    public static final String LOG_DATE = "nuodb.migrator.log.date";

    public static final String LOG_DATE_PATTERN = "nuodb.migrator.log.date.pattern";

    public static final String EXECUTABLE = "com.nuodb.migrator.executable";

    public static final String CLASSPATH = "com.nuodb.migrator.classpath";

    public static final String BOOTABLE_CLASS = "com.nuodb.migrator.bootable.class";

    public static final String DEFAULT_BOOTABLE_CLASS = "com.nuodb.migrator.cli.CliHandler";

    public static final String CONTEXT_CLASS = "com.nuodb.migrator.context.class";

    public static final String DEFAULT_CONTEXT_CLASS = "com.nuodb.migrator.context.SimpleContext";

    public static final String VERSION = "com.nuodb.migrator.version";

    public static final String CONFIG = "com.nuodb.migrator.config";

    public static final String DEFAULT_CONFIG = "nuodb-migrator.properties";

    public static final String CONFIG_DIR = "conf";

    private static Config INSTANCE;

    public static Config getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PropertiesConfigLoader().loadConfig();
        }
        return INSTANCE;
    }

    public abstract String getProperty(String property);

    public abstract String getProperty(String property, String defaultValue);

    public abstract Properties getProperties();
}
