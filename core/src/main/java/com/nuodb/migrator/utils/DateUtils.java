/**
 * Copyright (c) 2002-2012, Hirondelle Systems
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
 * THIS SOFTWARE IS PROVIDED BY HIRONDELLE SYSTEMS ''AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL HIRONDELLE SYSTEMS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.nuodb.migrator.utils;

import java.lang.reflect.Array;

/**
 * Collected utilities for overriding {@link Object#toString},
 * {@link Object#equals}, and {@link Object#hashCode}, and implementing
 * {@link Comparable}.
 * <p/>
 * <P>
 * All Model Objects should override the above {@link Object} methods. All Model
 * Objects that are being sorted in code should implement {@link Comparable}.
 * <p/>
 * <P>
 * In general, it is easier to use this class with <em>object</em> fields
 * (<tt>String</tt>, <tt>Date</tt>, <tt>BigDecimal</tt>, and so on), instead of
 * <em>primitive</em> fields (<tt>int</tt>, <tt>boolean</tt>, and so on).
 * <p/>
 * <P>
 * See below for example implementations of :
 * <ul>
 * <li><a href="#ToString">debug()</a>
 * <li><a href="#HashCode">hashCode()</a>
 * <li><a href="#Equals">equals()</a>
 * <li><a href="#Comparable">compareTo()</a>
 * </ul>
 * <p/>
 * <a name="ToString">
 * <P>
 * <b>debug()</b><br>
 * This class is intended for the most common case, where <tt>debug</tt> is used
 * in an <em>informal</em> manner (usually for logging and stack traces). That
 * is, <span class="highlight"> the caller should not rely on the
 * <tt>debug()</tt> text returned by this class to define program logic.</span>
 * <p/>
 * <P>
 * Typical example :
 * 
 * <PRE>
 * &#064;Override
 * public String debug() {
 *     return Utils.toStringFor(this);
 * }
 * </PRE>
 * <p/>
 * <P>
 * There is one <em>occasional</em> variation, used only when two model objects
 * reference each other. To avoid a problem with cyclic references and infinite
 * looping, implement as :
 * 
 * <PRE>
 * &#064;Override
 * public String debug() {
 *     return Utils.toStringAvoidCyclicRefs(this, Product.class, "getId");
 * }
 * </PRE>
 * <p/>
 * Here, the usual behavior is overridden for any method in 'this' object which
 * returns a <tt>Product</tt> : instead of calling <tt>Product.debug()</tt>, the
 * return value of <tt>Product.getId()</tt> is used instead.
 * <p/>
 * <a name="HashCode">
 * <P>
 * <b>hashCode()</b><br>
 * Example of the simplest style :
 * 
 * <pre>
 * &#064;Override public int hashCode() {
 * return Utils.hashFor(getSignificantFields());
 * }
 * ...
 * private String fName;
 * private Boolean fIsActive;
 * private Object[] getSignificantFields(){
 * //any primitive fields can be placed in a wrapper Object
 * return new Object[]{fName, fIsActive};
 * }
 * </pre>
 * <p/>
 * <P>
 * <a name="GetSignificantFields"></a><span class="highlight">Since the
 * {@link Object#equals} and {@link Object#hashCode} methods are so closely
 * related, and should always refer to the same fields, defining a
 * <tt>private</tt> method to return the <tt>Object[]</tt> of significant fields
 * is highly recommended.</span> Such a method would be called by <em>both</em>
 * <tt>equals</tt> and <tt>hashCode</tt>.
 * <p/>
 * <P>
 * If an object is
 * <a href="http://www.javapractices.com/Topic29.cjp">immutable</a>, then the
 * result may be calculated once, and then cached, as a small performance
 * optimization :
 * 
 * <pre>
 * &#064;Override public int hashCode() {
 * if ( fHashCode == 0 ) {
 * fHashCode = Utils.hashFor(getSignificantFields());
 * }
 * return fHashCode;
 * }
 * ...
 * private String fName;
 * private Boolean fIsActive;
 * private int fHashCode;
 * private Object[] getSignificantFields(){
 * return new Object[]{fName, fIsActive};
 * }
 * </pre>
 * <p/>
 * The most verbose style does not require wrapping primitives in an
 * <tt>Object</tt>:
 * 
 * <pre>
 * &#064;Override
 * public int hashCode() {
 *     int result = Utils.HASH_SEED;
 *     // collect the contributions of various fields
 *     result = Utils.hash(result, fPrimitive);
 *     result = Utils.hash(result, fObject);
 *     result = Utils.hash(result, fArray);
 *     return result;
 * }
 * </pre>
 * <p/>
 * <a name="Equals">
 * <P>
 * <b>equals()</b><br>
 * Simplest example, in a class called <tt>Visit</tt> (this is the recommended
 * style):
 * 
 * <PRE>
 *  &#064;Override public boolean equals(Object aThat) { Boolean result = Utils.quickEquals(this, aThat);
 * if ( result == null ){ Visit that = (Visit) aThat; result = Utils.equalsFor(this.getSignificantFields(),
 * that.getSignificantFields()); } return result; } ... private final Code fRestaurantCode; private final Date
 * fLunchDate; private final String fMessage; private Object[] getSignificantFields(){ return new Object[]
 * {fRestaurantCode, fLunchDate, fMessage}; }
 * </PRE>
 * <p/>
 * Second example, in a class called <tt>Member</tt> :
 * 
 * <PRE>
 *  &#064;Override public boolean equals( Object aThat ) { if (
 * this == aThat ) return true; if ( !(aThat instanceof Member) ) return false; Member that = (Member)aThat; return
 * Utils.equalsFor(this.getSignificantFields(), that.getSignificantFields()); } ... private final String fName; private
 * final Boolean fIsActive; private final Code fDisposition; private Object[] getSignificantFields(){ return new
 * Object[]{fName, fIsActive, fDisposition}; }
 * </PRE>
 * 
 * See note above regarding
 * <a href="#GetSignificantFields">getSignificantFields()</a>.
 * <p/>
 * <P>
 * More verbose example, in a class called <tt>Planet</tt> :
 * 
 * <PRE>
 *  &#064;Override public boolean equals(Object
 * aThat){ if ( this == aThat ) return true; if ( !(aThat instanceof Planet) ) return false; Planet that =
 * (Planet)aThat; return EqualsUtil.areEqual(this.fPossiblyNullObject, that.fPossiblyNullObject) &&
 * EqualsUtil.areEqual(this.fCollection, that.fCollection) && EqualsUtil.areEqual(this.fPrimitive, that.fPrimitive) &&
 * Arrays.equals(this.fArray, that.fArray); //arrays are different! }
 * </PRE>
 * <p/>
 * <a name="Comparable">
 * <P>
 * <b>compareTo()</b><br>
 * The {@link Comparable} interface is distinct, since it is not an overridable
 * method of the {@link Object} class.
 * <p/>
 * <P>
 * Example use case of using <a href='#comparePossiblyNull(T, T,
 * hirondelle.web4j.model.Utils.NullsGo)'>comparePossiblyNull</a>, (where
 * <tt>EQUAL</tt> takes the value <tt>0</tt>) :
 * 
 * <PRE>
 *  public int compareTo(Movie aThat) { if ( this == aThat )
 * return EQUAL;
 * <p/>
 * int comparison = Utils.comparePossiblyNull(this.fDateViewed, aThat.fDateViewed, NullsGo.LAST); if ( comparison !=
 * EQUAL ) return comparison;
 * <p/>
 * //this field is never null comparison = this.fTitle.compareTo(aThat.fTitle); if ( comparison != EQUAL ) return
 * comparison;
 * <p/>
 * comparison = Utils.comparePossiblyNull(this.fRating, aThat.fRating, NullsGo.LAST); if ( comparison != EQUAL ) return
 * comparison;
 * <p/>
 * comparison = Utils.comparePossiblyNull(this.fComment, aThat.fComment, NullsGo.LAST); if ( comparison != EQUAL )
 * return comparison;
 * <p/>
 * return EQUAL; }
 * </PRE>
 *
 * @author Hirondelle Systems
 * @author with a contribution by an anonymous user of javapractices.com
 */
final class DateUtils {

    private static final int PRIME_NUMBER = 37;

    /**
     * Initial seed value for a <tt>hashCode</tt>.
     * <p/>
     * Contributions from individual fields are 'added' to this initial value.
     * (Using a non-zero value decreases collisons of <tt>hashCode</tt> values.)
     */
    private static final int HASH_SEED = 23;

    private static final String SINGLE_QUOTE = "'";

    private DateUtils() {
    }

    private static int firstTerm(int aSeed) {
        return PRIME_NUMBER * aSeed;
    }

    private static boolean isArray(Object aObject) {
        return aObject != null && aObject.getClass().isArray();
    }

    static boolean isNotBlank(String text) {
        return (text != null) && (text.trim().length() > 0);
    }

    static String quote(Object aObject) {
        return SINGLE_QUOTE + String.valueOf(aObject) + SINGLE_QUOTE;
    }

    /**
     * Return the hash code in a single step, using all significant fields
     * passed in an {@link Object} sequence parameter.
     * <p/>
     * <P>
     * (This is the recommended way of implementing <tt>hashCode</tt>.)
     * <p/>
     * <P>
     * Each element of <tt>aFields</tt> must be an {@link Object}, or an array
     * containing possibly-null <tt>Object</tt>s. These items will each
     * contribute to the result. (It is not a requirement to use <em>all</em>
     * fields related to an object.)
     * <p/>
     * <P>
     * If the caller is using a <em>primitive</em> field, then it must be
     * converted to a corresponding wrapper object to be included in
     * <tt>aFields</tt>. For example, an <tt>int</tt> field would need
     * conversion to an {@link Integer} before being passed to this method.
     */
    static final int hash(Object... aFields) {
        int result = HASH_SEED;
        for (Object field : aFields) {
            result = hash(result, field);
        }
        return result;
    }

    /**
     * Hash code for <tt>int</tt> primitives.
     * <P>
     * Note that <tt>byte</tt> and <tt>short</tt> are also handled by this
     * method, through implicit conversion.
     */
    static int hash(int aSeed, int aInt) {
        return firstTerm(aSeed) + aInt;
    }

    /**
     * Hash code for an Object.
     * <p/>
     * <P>
     * <tt>aObject</tt> is a possibly-null object field, and possibly an array.
     * <p/>
     * If <tt>aObject</tt> is an array, then each element may be a primitive or
     * a possibly-null object.
     */
    static int hash(int aSeed, Object aObject) {
        int result = aSeed;
        if (aObject == null) {
            result = hash(result, 0);
        } else if (!isArray(aObject)) {
            result = hash(result, aObject.hashCode());
        } else {
            int length = Array.getLength(aObject);
            for (int idx = 0; idx < length; ++idx) {
                Object item = Array.get(aObject, idx);
                // recursive call!
                result = hash(result, item);
            }
        }
        return result;
    }

    // EQUALS //

    /**
     * Quick checks for <em>possibly</em> determining equality of two objects.
     * <p/>
     * <P>
     * This method exists to make <tt>equals</tt> implementations readIndexes
     * more legibly, and to avoid multiple <tt>return</tt> statements.
     * <p/>
     * <P>
     * <em>It cannot be used by itself to fully implement <tt>equals</tt>. </em>
     * It uses <tt>==</tt> and <tt>instanceof</tt> to determine if equality can
     * be found cheaply, without the need to examine field values in detail. It
     * is <em>always</em> paired with some other method (usually
     * {@link #equalsFields(Object[], Object[])}), as in the following example :
     * 
     * <PRE>
     *  public boolean equals(Object aThat){ Boolean result = Utils.quickEquals(this,
     * aThat); <b>if ( result == null ){</b> //quick checks not sufficient to determine equality, //so a full
     * field-by-field check is needed : This this = (This) aThat; //will not fail result =
     * Utils.equalsFor(this.getSignificantFields(), that.getSignificantFields()); } return result; }
     * </PRE>
     * <p/>
     * <P>
     * This method is unusual since it returns a <tt>Boolean</tt> that takes
     * <em>3</em> values : <tt>true</tt>, <tt>false</tt>, and <tt>null</tt>.
     * Here, <tt>true</tt> and <tt>false</tt> mean that a simple quick check was
     * able to determine equality. <span class='highlight'>The <tt>null</tt>
     * case means that the quick checks were not able to determine if the
     * objects are equal or not, and that further field-by-field examination is
     * necessary. The caller must always perform a check-for-null on the return
     * value.</span>
     */
    static Boolean equals(Object aThis, Object aThat) {
        Boolean result = null;
        if (aThis == aThat) {
            result = Boolean.TRUE;
        } else {
            Class<?> thisClass = aThis.getClass();
            if (!thisClass.isInstance(aThat)) {
                result = Boolean.FALSE;
            }
        }
        return result;
    }

    /**
     * Return the result of comparing all significant fields.
     * <p/>
     * <P>
     * Both <tt>Object[]</tt> parameters are the same size. Each includes all
     * fields that have been deemed by the caller to contribute to the
     * <tt>equals</tt> method. <em>None of those fields are array fields.</em>
     * The order is the same in both arrays, in the sense that the Nth item in
     * each array corresponds to the same underlying field. The caller controls
     * the order in which fields are compared simply through the iteration order
     * of these two arguments.
     * <p/>
     * <P>
     * If a primitive field is significant, then it must be converted to a
     * corresponding wrapper <tt>Object</tt> by the caller.
     */
    static boolean equalsFields(Object[] aThisSignificantFields, Object[] aThatSignificantFields) {
        // (varargs can be used for final arg only)
        if (aThisSignificantFields.length != aThatSignificantFields.length) {
            throw new IllegalArgumentException(
                    "Array lengths do not match. 'This' length is " + aThisSignificantFields.length
                            + ", while 'That' length is " + aThatSignificantFields.length + ".");
        }

        boolean result = true;
        for (int idx = 0; idx < aThisSignificantFields.length; ++idx) {
            if (!areEqual(aThisSignificantFields[idx], aThatSignificantFields[idx])) {
                result = false;
                break;
            }
        }
        return result;
    }

    /**
     * Equals for possibly-<tt>null</tt> object field.
     * <p/>
     * <P>
     * <em>Does not include arrays</em>. (This restriction will likely be
     * removed in a future version.)
     */
    static boolean areEqual(Object aThis, Object aThat) {
        if (isArray(aThis) || isArray(aThat)) {
            throw new IllegalArgumentException("This method does not currently support arrays.");
        }
        return aThis == null ? aThat == null : aThis.equals(aThat);
    }

    /**
     * Define hows <tt>null</tt> items are treated in a comparison. Controls if
     * <tt>null</tt> items appear first or last.
     * <p/>
     * <P>
     * See <a href='#comparePossiblyNull(T, T,
     * hirondelle.web4j.model.Utils.NullsGo)'>comparePossiblyNull</a>.
     */
    enum NullsGo {
        FIRST, LAST
    }

    /**
     * Utility for implementing {@link Comparable}. See
     * <a href='#Comparable'>class example</a> for illustration.
     * <p/>
     * <P>
     * The {@link Comparable} interface specifies that
     * 
     * <PRE>
     * blah.compareTo(null)
     * </PRE>
     * 
     * should throw a {@link NullPointerException}. You should follow that
     * guideline. Note that this utility method itself accepts nulls
     * <em>without</em> throwing a {@link NullPointerException}. In this way,
     * this method can handle nullable fields just like any other field.
     * <p/>
     * <P>
     * There are <a href=
     * 'http://www.javapractices.com/topic/TopicAction.do?Id=207'>special
     * issues</a> for sorting {@link String}s regarding case,
     * {@link java.util.Locale}, and accented characters.
     *
     * @param aThis
     *            an object that implements {@link Comparable}
     * @param aThat
     *            an object of the same type as <tt>aThis</tt>
     * @param nullsGo
     *            defines if <tt>null</tt> items should be placed first or last
     */
    static <T extends Comparable<T>> int comparePossiblyNull(T aThis, T aThat, NullsGo nullsGo) {
        int EQUAL = 0;
        int BEFORE = -1;
        int AFTER = 1;
        int result = EQUAL;

        if (aThis != null && aThat != null) {
            result = aThis.compareTo(aThat);
        } else {
            // at least one reference is null - special handling
            if (aThis == null && aThat == null) {
                // not distinguishable, so treat as equal
            } else if (aThis == null && aThat != null) {
                result = BEFORE;
            } else if (aThis != null && aThat == null) {
                result = AFTER;
            }
            if (NullsGo.LAST == nullsGo) {
                result = (-1) * result;
            }
        }
        return result;
    }
}
