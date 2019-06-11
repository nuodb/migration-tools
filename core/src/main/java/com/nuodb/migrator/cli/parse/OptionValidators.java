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

import java.util.ListIterator;

/**
 * @author Sergey Bushik
 */
public class OptionValidators {

    public static OptionProcessor toOptionProcessor(OptionValidator optionValidator) {
        return new OptionProcessorValidator(optionValidator);
    }

    static class OptionProcessorValidator implements OptionProcessor {

        private OptionValidator optionValidator;

        public OptionProcessorValidator(OptionValidator optionValidator) {
            this.optionValidator = optionValidator;
        }

        @Override
        public void preProcess(CommandLine commandLine, Option option, ListIterator<String> arguments) {
        }

        @Override
        public void process(CommandLine commandLine, Option option, ListIterator<String> arguments) {
        }

        @Override
        public void postProcess(CommandLine commandLine, Option option) {
            if (optionValidator.canValidate(commandLine, option)) {
                optionValidator.validate(commandLine, option);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof OptionProcessorValidator))
                return false;
            OptionProcessorValidator that = (OptionProcessorValidator) o;

            if (optionValidator != null ? !optionValidator.equals(that.optionValidator)
                    : that.optionValidator != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return optionValidator != null ? optionValidator.hashCode() : 0;
        }
    }
}
