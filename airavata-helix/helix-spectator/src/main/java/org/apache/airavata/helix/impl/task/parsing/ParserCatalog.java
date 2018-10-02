package org.apache.airavata.helix.impl.task.parsing;

import java.util.Set;

public class ParserCatalog {

    public static ParserInfo getParserById(String parserId) throws Exception {
        //todo perform validations if necessary
        return CatalogUtil.parserCatalogLookup(parserId);
        // just find the catlog entry reading the json catalog file, it is no needed to load entire catalog to the memory
        // only load when it is required
    }

    public static Set<ParsingTemplate> getParserTemplatesForApplication(String appIfaceId) throws Exception {
        // all the templates should be run
        return CatalogUtil.dagCatalogLookup(appIfaceId);
    }

    // this class is something similar to the CatalogUtil classs
    // This is a helper class for methods look at the two different methods they are just helping methods
}
