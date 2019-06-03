/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nuodb.migrator.cli.parse.help;

import com.nuodb.migrator.cli.parse.Help;
import com.nuodb.migrator.cli.parse.HelpHint;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static com.nuodb.migrator.cli.parse.HelpHint.*;
import static com.nuodb.migrator.utils.ValidationUtils.isNotNull;
import static java.lang.System.getProperty;
import static java.util.Collections.unmodifiableSet;
import static org.slf4j.LoggerFactory.getLogger;

@SuppressWarnings("unchecked")
public class HelpFormatter {

    public final static String GUTTER = "    ";

    private static final String LINE_SEPARATOR = getProperty("line.separator");

    public static Set<HelpHint> USAGE_OUTPUT_HINTS;

    public static Set<HelpHint> HELP_OUTPUT_HINTS;

    public static Set<HelpHint> OPTION_OUTPUT_HINTS;

    static {
        USAGE_OUTPUT_HINTS = unmodifiableSet(newHashSet(ALIASES, OPTIONAL, PROPERTY, SWITCH, GROUP_OPTIONS,
                GROUP_ARGUMENTS, GROUP_OUTER, ARGUMENT_BRACKETED, AUGMENT_ARGUMENT, AUGMENT_GROUP));
        HELP_OUTPUT_HINTS = unmodifiableSet(newHashSet(ALIASES, OPTIONAL, GROUP, AUGMENT_ARGUMENT));
        OPTION_OUTPUT_HINTS = unmodifiableSet(newHashSet(ALIASES, OPTIONAL, OPTIONAL_CHILD_GROUP, PROPERTY, SWITCH,
                GROUP, GROUP_OPTIONS, GROUP_ARGUMENTS, GROUP_OUTER, ARGUMENT_BRACKETED, AUGMENT_GROUP));
    }

    protected transient final Logger logger = getLogger(getClass());
    protected Set<HelpHint> usageOutputHints = newHashSet(USAGE_OUTPUT_HINTS);
    protected Set<HelpHint> helpOutputHints = newHashSet(HELP_OUTPUT_HINTS);
    protected Set<HelpHint> optionOutputHints = newHashSet(OPTION_OUTPUT_HINTS);

    protected String executable;
    protected String header;
    protected String divider;
    protected String gutter = GUTTER;
    protected String footer;
    protected Option option;
    protected Comparator comparator;
    protected OptionException exception;

    public void format(OutputStream output) {
        format(new PrintWriter(output));
    }

    public void format(Writer writer) {
        try {
            header(writer);
            exception(writer);
            usage(writer);
            help(writer);
            footer(writer);
            writer.flush();
        } catch (IOException exception) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed formatting help", exception);
            }
        }
    }

    protected void header(Writer writer) throws IOException {
        if (getHeader() != null) {
            divider(writer);
            line(writer, getHeader());
        }
    }

    protected void exception(Writer writer) throws IOException {
        OptionException exception = getException();
        if (exception != null) {
            divider(writer);
            line(writer, exception.getMessage());
            line(writer);
        }
    }

    protected void divider(Writer writer) throws IOException {
        if (getDivider() != null) {
            line(writer, getDivider());
        }
    }

    protected void usage(Writer writer) throws IOException {
        divider(writer);
        StringBuilder usage = new StringBuilder("Usage:\n");
        usage.append(getExecutable()).append(" ");
        Option option;
        OptionException exception = getException();
        if (exception != null && exception.getOption() != null) {
            option = exception.getOption();
        } else {
            option = getOption();
        }
        isNotNull(option, "Option is required");
        option.help(usage, getUsageOutputHints(), getComparator());
        line(writer, usage.toString());
    }

    protected void help(Writer writer) throws IOException {
        line(writer, getDivider());
        Option option;
        OptionException exception = getException();
        if (exception != null && exception.getOption() != null) {
            option = exception.getOption();
        } else {
            option = getOption();
        }
        isNotNull(option, "Option is required");
        List<Help> helps = option.help(0, getOptionOutputHints(), getComparator());
        int usageWidth = 0;
        for (Help help : helps) {
            String content = help.help(getHelpOutputHints(), getComparator());
            usageWidth = Math.max(usageWidth, content.length());
        }
        for (Help help : helps) {
            line(writer, pad(help.help(getHelpOutputHints(), getComparator()), usageWidth), getGutter(),
                    help.getOption().getDescription());
        }
        line(writer, getDivider());
    }

    protected void footer(Writer writer) throws IOException {
        if (getFooter() != null) {
            line(writer, getFooter());
            line(writer, getDivider());
        }
    }

    protected void line(Writer writer, String... chunks) throws IOException {
        for (String chunk : chunks) {
            if (chunk != null) {
                writer.write(chunk);
            }
        }
        writer.write(LINE_SEPARATOR);
    }

    protected static String pad(String text, int width) {
        StringBuilder padded = new StringBuilder();
        if (text != null) {
            padded.append(text);
        }
        int left = padded.length();
        for (int i = left; i < width; ++i) {
            padded.append(' ');
        }
        return padded.toString();
    }

    public Set<HelpHint> getUsageOutputHints() {
        return usageOutputHints;
    }

    public void setUsageOutputHints(Set<HelpHint> usageOutputHints) {
        this.usageOutputHints = usageOutputHints;
    }

    public Set<HelpHint> getHelpOutputHints() {
        return helpOutputHints;
    }

    public void setHelpOutputHints(Set<HelpHint> helpOutputHints) {
        this.helpOutputHints = helpOutputHints;
    }

    public Set<HelpHint> getOptionOutputHints() {
        return optionOutputHints;
    }

    public void setOptionOutputHints(Set<HelpHint> optionOutputHints) {
        this.optionOutputHints = optionOutputHints;
    }

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public String getHeader() {
        return header;
    }

    public void setHeader(String header) {
        this.header = header;
    }

    public String getDivider() {
        return divider;
    }

    public void setDivider(String divider) {
        this.divider = divider;
    }

    public String getGutter() {
        return gutter;
    }

    public void setGutter(String gutter) {
        this.gutter = gutter;
    }

    public String getFooter() {
        return footer;
    }

    public void setFooter(String footer) {
        this.footer = footer;
    }

    public Option getOption() {
        return option;
    }

    public void setOption(Option option) {
        this.option = option;
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    public OptionException getException() {
        return exception;
    }

    public void setException(OptionException exception) {
        this.exception = exception;
    }
}