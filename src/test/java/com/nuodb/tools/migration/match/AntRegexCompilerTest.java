package com.nuodb.tools.migration.match;

import junit.framework.Assert;
import org.junit.Test;

public class AntRegexCompilerTest {

    private static final String PATTERN = "TEST*";

    @Test
    public void testMatcherCreation() throws Exception {
        final Regex matcher = (new AntRegexCompiler()).compile(PATTERN);
        Assert.assertNotNull(matcher);
        Assert.assertNotNull((new AntRegexCompiler()).compile("*TEST*"));
        Assert.assertEquals(matcher.regex(), PATTERN);
    }

    @Test
    public void testMatch() throws Exception {
        AntRegexCompiler compiler = new AntRegexCompiler();
        Regex regex = compiler.compile(PATTERN);
        Assert.assertTrue(regex.test("TEST3"));
        Assert.assertTrue(regex.test("TEST"));
        Assert.assertTrue(regex.test("TEST tt"));
        Assert.assertFalse(regex.test("TES tt"));
        Assert.assertFalse(regex.test(""));
        Assert.assertFalse(regex.test("test1"));
    }
}
