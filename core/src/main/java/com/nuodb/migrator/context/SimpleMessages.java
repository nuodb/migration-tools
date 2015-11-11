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
package com.nuodb.migrator.context;

import com.nuodb.migrator.i18n.Messages;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import static java.util.Locale.getDefault;
import static java.util.ResourceBundle.getBundle;

/**
 * @author Sergey Bushik
 */
public class SimpleMessages implements Messages {

    public static final String DEFAULT_BUNDLE = "com.nuodb.migrator.i18n.messages";

    private static SimpleMessages INSTANCE;

    private ResourceBundle bundle;

    public static Messages getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new SimpleMessages(DEFAULT_BUNDLE);
        }
        return INSTANCE;
    }

    public SimpleMessages(String bundle) {
        int first = bundle.indexOf('_');
        int second = bundle.indexOf('_', first + 1);
        Locale locale;
        if (first != -1) {
            String language = bundle.substring(first + 1, second);
            String country = bundle.substring(second + 1);
            locale = new Locale(language, country);
        } else {
            locale = getDefault();
        }
        try {
            this.bundle = getBundle(bundle, locale);
        } catch (MissingResourceException exp) {
            this.bundle = getBundle(DEFAULT_BUNDLE, locale);
        }
    }

    public String getMessage(String code, Object... values) {
        String message = bundle.getString(code);
        if (values != null && values.length > 0) {
            return new MessageFormat(message).format(values);
        } else {
            return message;
        }
    }
}
