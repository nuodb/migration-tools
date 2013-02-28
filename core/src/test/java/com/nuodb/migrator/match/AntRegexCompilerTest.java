package com.nuodb.migrator.match;

import junit.framework.Assert;
import org.junit.Test;

public class AntRegexCompilerTest {

    private static final String PATTERN = "TEST*";

    @Test
    public void testMatcherCreation() throws Exception {
        RegexCompiler compiler = AntRegexCompiler.INSTANCE;
        final Regex matcher = compiler.compile(PATTERN);
        Assert.assertNotNull(matcher);
        Assert.assertNotNull(compiler.compile("*TEST*"));
        Assert.assertEquals(matcher.regex(), PATTERN);
    }

    @Test
    public void testMatch() throws Exception {
        RegexCompiler compiler = AntRegexCompiler.INSTANCE;
        Regex regex = compiler.compile(PATTERN);
        Assert.assertTrue(regex.test("TEST3"));
        Assert.assertTrue(regex.test("TEST"));
        Assert.assertTrue(regex.test("TEST tt"));
        Assert.assertFalse(regex.test("TES tt"));
        Assert.assertFalse(regex.test(""));
        Assert.assertFalse(regex.test("test1"));
    }
}
