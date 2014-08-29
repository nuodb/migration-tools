/**
 * Copyright (c) 2014, NuoDB, Inc.
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
package com.nuodb.migrator.cli.run;

import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionSet;
import com.nuodb.migrator.cli.parse.option.GroupBuilder;
import com.nuodb.migrator.spec.ResourceSpec;
import com.nuodb.migrator.spec.SchemaJobSpec;

import java.util.Map;

import static com.nuodb.migrator.context.ContextUtils.getMessage;

/**
 * @author Sergey Bushik
 */
@SuppressWarnings("PointlessBooleanExpression")
public class CliSchemaJob extends CliJob<SchemaJobSpec> {

    public CliSchemaJob() {
        super(SCHEMA);
    }

    @Override
    protected Option createOption() {
        GroupBuilder group = newGroupBuilder().
                withName(getMessage(SCHEMA_GROUP_NAME));
        group.withRequired(true);
        group.withOption(createSourceGroup());
        group.withOption(createTargetGroup());
        group.withOption(createOutputGroup());
        group.withOption(createSchemaMigrationGroup());
        return group.build();
    }

    @Override
    protected void bind(OptionSet optionSet) {
        SchemaJobSpec schemaJobSpec = new SchemaJobSpec();
        schemaJobSpec.setSourceSpec(parseSourceGroup(optionSet, this));
        schemaJobSpec.setTargetSpec(parseTargetGroup(optionSet, this));
        schemaJobSpec.setOutputSpec(parseOutputGroup(optionSet, this));
        parseSchemaMigrationGroup(optionSet, schemaJobSpec, this);
        setJobSpec(schemaJobSpec);
    }

    @Override
    public void execute(Map<Object, Object> context) {
        getMigrator().execute(getJobSpec(), context);
    }

    @Override
    protected Group createOutputGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(SCHEMA_OUTPUT_GROUP_NAME));
        Option path = newBasicOptionBuilder().
                withName(OUTPUT_PATH).
                withRequired(true).
                withDescription(getMessage(OUTPUT_PATH_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(OUTPUT_PATH_ARGUMENT_NAME)).
                                withMinimum(1).
                                withRequired(true).build()
                ).build();
        group.withOption(path);
        return group.build();
    }

    @Override
    protected ResourceSpec parseOutputGroup(OptionSet optionSet, Option option) {
        ResourceSpec resource = null;
        if (optionSet.hasOption(OUTPUT_PATH)) {
            resource = new ResourceSpec();
            resource.setPath((String) optionSet.getValue(OUTPUT_PATH));
        }
        return resource;
    }

    @Override
    protected Group createTargetGroup() {
        GroupBuilder group = newGroupBuilder().withName(getMessage(TARGET_GROUP_NAME));

        Option url = newBasicOptionBuilder().
                withName(TARGET_URL).
                withDescription(getMessage(TARGET_URL_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_URL_ARGUMENT_NAME)).
                                withMinimum(1).withRequired(true).build()
                ).build();
        group.withOption(url);

        Option username = newBasicOptionBuilder().
                withName(TARGET_USERNAME).
                withDescription(getMessage(TARGET_USERNAME_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_USERNAME_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(username);

        Option password = newBasicOptionBuilder().
                withName(TARGET_PASSWORD).
                withDescription(getMessage(TARGET_PASSWORD_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_PASSWORD_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(password);

        Option properties = newBasicOptionBuilder().
                withName(TARGET_PROPERTIES).
                withDescription(getMessage(TARGET_PROPERTIES_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_PROPERTIES_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(properties);

        Option schema = newBasicOptionBuilder().
                withName(TARGET_SCHEMA).
                withDescription(getMessage(TARGET_SCHEMA_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_SCHEMA_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(schema);

        Option autoCommit = newBasicOptionBuilder().
                withName(TARGET_AUTO_COMMIT).
                withDescription(getMessage(TARGET_AUTO_COMMIT_OPTION_DESCRIPTION)).
                withArgument(
                        newArgumentBuilder().
                                withName(getMessage(TARGET_AUTO_COMMIT_ARGUMENT_NAME)).build()
                ).build();
        group.withOption(autoCommit);
        return group.build();
    }
}