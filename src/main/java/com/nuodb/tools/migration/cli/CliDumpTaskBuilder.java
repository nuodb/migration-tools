/**
 * Copyright (c) 2012, NuoDB, Inc.
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
package com.nuodb.tools.migration.cli;

import com.nuodb.tools.migration.cli.handler.Group;
import com.nuodb.tools.migration.cli.handler.Option;
import com.nuodb.tools.migration.cli.handler.toolkit.OptionToolkit;
import com.nuodb.tools.migration.i18n.Resources;

/**
 * @author Sergey Bushik
 */
public class CliDumpTaskBuilder implements CliConstants {

    private Resources resources = Resources.getResources();

    private OptionToolkit toolkit;

    public CliDumpTaskBuilder(OptionToolkit toolkit) {
        this.toolkit = toolkit;
    }

    public Group build() {
        return toolkit.newGroup().
                withName(resources.getMessage(DUMP_GROUP_KEY)).
                withOption(buildSource(toolkit)).
                withOption(buildOutput(toolkit)).build();
    }

    /**
     * Builds the option of options for the source database connection.
     *
     * @param toolkit command line toolkit
     * @return option of options for the source database.
     */
    protected Option buildSource(OptionToolkit toolkit) {
        Option driver = toolkit.newOption().
                withName(SOURCE_DRIVER_OPTION).
                withDescription(resources.getMessage(SOURCE_DRIVER_OPTION_KEY)).
                withRequired(true).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_DRIVER_ARGUMENT_KEY)).build()
                ).build();
        Option url = toolkit.newOption().
                withName(SOURCE_URL_OPTION).
                withDescription(resources.getMessage(SOURCE_URL_OPTION_KEY)).
                withRequired(true).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_URL_ARGUMENT_KEY)).build()
                ).build();
        Option username = toolkit.newOption().
                withName(SOURCE_USERNAME_OPTION).
                withDescription(resources.getMessage(SOURCE_USERNAME_OPTION_KEY)).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_USERNAME_ARGUMENT_KEY)).build()
                ).build();
        Option password = toolkit.newOption().
                withName(SOURCE_PASSWORD_OPTION).
                withDescription(resources.getMessage(SOURCE_PASSWORD_OPTION_KEY)).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_PASSWORD_ARGUMENT_KEY)).build()
                ).build();
        Option properties = toolkit.newOption().
                withName(SOURCE_PROPERTIES_OPTION).
                withDescription(resources.getMessage(SOURCE_PROPERTIES_OPTION_KEY)).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_PROPERTIES_ARGUMENT_KEY)).build()
                ).build();
        Option catalog = toolkit.newOption().
                withName(SOURCE_CATALOG_OPTION).
                withDescription(resources.getMessage(SOURCE_CATALOG_OPTION_KEY)).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_CATALOG_ARGUMENT_KEY)).build()
                ).build();
        Option schema = toolkit.newOption().
                withName(SOURCE_SCHEMA_OPTION).
                withDescription(resources.getMessage(SOURCE_SCHEMA_OPTION_KEY)).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(SOURCE_SCHEMA_ARGUMENT_KEY)).build()
                ).build();
        return toolkit.newGroup().
                withName(resources.getMessage(SOURCE_GROUP_KEY)).
                withRequired(true).
                withMinimum(1).
                withOption(driver).
                withOption(url).
                withOption(username).
                withOption(password).
                withOption(properties).
                withOption(catalog).
                withOption(schema).build();
    }

    protected Option buildOutput(OptionToolkit toolkit) {
        Option type = toolkit.newOption().
                withName(OUTPUT_TYPE_OPTION).
                withDescription(resources.getMessage(OUTPUT_TYPE_OPTION_KEY)).
                withRequired(true).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_TYPE_ARGUMENT_KEY)).build()
                ).build();
        Option path = toolkit.newOption().
                withName(OUTPUT_PATH_OPTION).
                withDescription(resources.getMessage(OUTPUT_PATH_OPTION_KEY)).
                withRequired(true).
                withArgument(
                        toolkit.newArgument().
                                withName(resources.getMessage(OUTPUT_PATH_ARGUMENT_KEY)).build()
                ).build();
        return toolkit.newGroup().
                withName(resources.getMessage(OUTPUT_GROUP_KEY)).
                withRequired(true).
                withMinimum(1).
                withOption(type).
                withOption(path).build();
    }
}
