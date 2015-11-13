/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.util;

import java.util.HashMap;
import java.util.Map;

public class CommandResultProperties {

    private static final String TAG_Group_Properties_Begin_Char = "#";
    private static final String TAG_Key_Value_Split_Char = ":";

    //key:value=Group_Properties:Group_Properties<Map,String>
    protected Map<String, Map<String, String>> attributes = new HashMap<String, Map<String, String>>();

    public void loadProperties(String cmdResult) {

        if ((cmdResult == null) || cmdResult.trim().equals("")) {
            return;
        }
        try {
            String[] lines = cmdResult.split("\n");
            String groupProperties = null;
            for (String line : lines) {
                //find and update the group properties name
                if ((line != null) && (line.indexOf(TAG_Group_Properties_Begin_Char) == 0)) {
                    groupProperties = line.substring(1, line.length()).trim();
                }

                //find and add the properties
                if ((line != null) && (line.indexOf(TAG_Key_Value_Split_Char) > 0)) {
                    String[] pairs = line.trim().split(TAG_Key_Value_Split_Char);
                    if ((groupProperties != null) && !groupProperties.equals("") && (pairs != null)
                            && (pairs.length == 2)) {
                        setAttribute(groupProperties, pairs[0], pairs[1]);
                    }
                }
            }
        } catch (Exception e) {
            //consume the exception
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute() {
        return (T) this.attributes;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String groupProperties, String property) {
        Map<String, String> pros = attributes.get(groupProperties);
        if (pros != null) {
            return (T) pros.get(property);
        }
        return null;
    }

    public void setAttribute(String groupProperties, String property, String value) {
        Map<String, String> pros = attributes.get(groupProperties);
        if (pros == null) {
            pros = new HashMap<String, String>();
            attributes.put(groupProperties, pros);
        }
        pros.put(property, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T getGroupProperties(String groupProperties) {
        return (T) attributes.get(groupProperties);
    }

    public void setGroupProperties(String groupProperties, Map<String, String> pros) {
        attributes.put(groupProperties, pros);
    }
}
