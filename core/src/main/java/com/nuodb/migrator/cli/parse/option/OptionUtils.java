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
package com.nuodb.migrator.cli.parse.option;

import com.nuodb.migrator.cli.parse.Argument;
import com.nuodb.migrator.cli.parse.Group;
import com.nuodb.migrator.cli.parse.Option;
import com.nuodb.migrator.cli.parse.OptionException;

import static java.lang.String.format;

/**
 * @author Sergey Bushik
 */
public class OptionUtils {

    public static String quote(String argument) {
        return "\"" + argument + "\"";
    }

    public static String unquote(String argument) {
        if (!argument.startsWith("\"") || !argument.endsWith("\"")) {
            return argument;
        }
        return argument.substring(1, argument.length() - 1);
    }

    public static void groupMinimum(Group group) {
        throw new OptionException("Missing option", group);
    }

    public static void groupMaximum(Group group, Option option) {
        throw new OptionException(format("Unexpected option %s", option.getName()), group);
    }

    public static void optionRequired(Option option) {
        throw new OptionException(format("Missing required option %s", option.getName()), option);
    }

    public static void optionUnexpected(Option option, String argument) {
        throw new OptionException(format("Unexpected token %s for %s option", argument, option.getName()), option);
    }

    public static void argumentMinimum(Option option, Argument argument) {
        throw new OptionException(format("Missing %s argument for %s option", argument.getName(), option.getName()),
                option);
    }

    public static void argumentMaximum(Option option, Argument argument) {
        throw new OptionException(format("Too many %s arguments for %s option", argument.getName(), option.getName()),
                option);
    }
}
