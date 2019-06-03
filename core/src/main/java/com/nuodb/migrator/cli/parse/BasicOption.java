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
package com.nuodb.migrator.cli.parse;

import com.nuodb.migrator.cli.parse.option.OptionFormat;

import java.util.Map;

/**
 * Basic option which may be aliased with additional names. Aliases triggers
 * option processing in a line with its name. Having option --opt it can be
 * aliased to -o using short form (single hyphen notation) as:
 *
 * <pre>
 * BasicOption option;
 * option.setName("opt");
 * option.addAlias("o", OptionFormat.SHORT);
 * </pre>
 *
 * @author Sergey Bushik
 */
public interface BasicOption extends AugmentOption {

    /**
     * Adds alias to the option in the default option format
     *
     * @param alias
     *            to add to this option
     */
    void addAlias(String alias);

    /**
     *
     * @param alias
     * @param optionFormat
     */
    void addAlias(String alias, OptionFormat optionFormat);

    Map<String, OptionFormat> getAliases();

    void setAliases(Map<String, OptionFormat> aliases);
}
