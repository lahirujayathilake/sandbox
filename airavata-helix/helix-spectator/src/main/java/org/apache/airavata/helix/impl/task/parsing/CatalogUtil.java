/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.airavata.helix.impl.task.parsing;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.airavata.common.utils.ServerSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Util class for Catalog related tasks
 *
 * @since 1.0.0-SNAPSHOT
 */
public class CatalogUtil {

    private final static Logger logger = LoggerFactory.getLogger(CatalogUtil.class);

    /**
     * Creates list of {@link ParserInfo}s using the catalog DB
     * which has the details of Docker parsers
     *
     * @return a list of {@link ParserInfo}s
     * @throws FileNotFoundException if Catalog could not be found
     */
    public static ParserInfo parserCatalogLookup(String parserId) throws Exception {
        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(new FileReader(ServerSettings.getSetting("parser.catalog.path")), JsonArray.class);

        for (JsonElement element : jsonArray) {
            JsonObject obj = element.getAsJsonObject();
            String id = obj.get("id").getAsString();
            //todo assumption --> there should be at least one input file and output file
            //todo better to go for a map like something.. singleton approach --> find for a best way for something like this

            if (id.equals(parserId)) {
                List<String> inputFiles = null;
                List<String> mandatoryOutputFiles = null;
                List<String> optionalOutputFiles = null;
                List<String> envVariables = null;

                Type listType = new TypeToken<ArrayList<String>>() {
                }.getType();

                if (!obj.get("inputFiles").isJsonNull()) {
                    inputFiles = gson.fromJson(obj.get("inputFiles").getAsJsonArray(), listType);
                }
                if (!obj.get("mandatoryOutputFiles").isJsonNull()) {
                    mandatoryOutputFiles = gson.fromJson(obj.get("mandatoryOutputFiles").getAsJsonArray(), listType);
                }
                if (!obj.get("optionalOutputFiles").isJsonNull()) {
                    optionalOutputFiles = gson.fromJson(obj.get("optionalOutputFiles").getAsJsonArray(), listType);
                }
                if (!obj.get("envVariables").isJsonNull()) {
                    envVariables = gson.fromJson(obj.get("envVariables").getAsJsonArray(), listType);
                }

                return new ParserInfo.Builder(
                        id,
                        obj.get("dockerImageName").getAsString(),
                        obj.get("dockerWorkingDirPath").getAsString(),
                        obj.get("executableBinary").getAsString(),
                        obj.get("executingFile").getAsString(),
                        inputFiles,
                        mandatoryOutputFiles)
                        .optionalOutputFiles(optionalOutputFiles)
                        .runInDetachedMode(obj.get("runInDetachedMode").getAsString())
                        .automaticallyRmContainer(obj.get("automaticallyRmContainer").getAsString())
                        .runInDetachedMode(obj.get("runInDetachedMode").getAsString())
                        .securityOpt(obj.get("securityOpt").getAsString())
                        .envVariables(envVariables)
                        .cpus(obj.get("cpus").getAsString())
                        .label(obj.get("label").getAsString())
                        .build();
            }
        }
        throw new Exception("Could not found the Parser for ParserId: " + parserId);
    }

    /**
     * Converts the given <code>entry</code> to a JSON String
     *
     * @param entry {@link ParserInfo} should be converted to JSON string
     * @return JSON string of the <code>entry</code>
     */
    @Deprecated
    public static String catalogEntryToJSONString(ParserInfo entry) {
        return new Gson().toJson(entry);
    }

    /**
     * Converts the given <code>strEntry</code> to a {@link ParserInfo}
     *
     * @param strEntry which should be used to generate the relevant {@link ParserInfo}
     * @return the {@link ParserInfo} corresponding to the <code>strEntry</code>
     */
    @Deprecated
    public static ParserInfo jsonStringToCatalogEntry(String strEntry) {
        return new Gson().fromJson(strEntry, ParserInfo.class);
    }

    public static Set<ParsingTemplate> dagCatalogLookup(String appIfaceId) throws Exception {
        Set<ParsingTemplate> templates = new HashSet<>();
        Gson gson = new Gson();
        JsonArray jsonArray = gson.fromJson(new FileReader(ServerSettings.getSetting("parsing.template.path")), JsonArray.class);
        Type listType = new TypeToken<ArrayList<ParsingTemplate>>() {
        }.getType();

        for (JsonElement element : jsonArray) {
            List<String> envVariables = null;
            JsonObject obj = element.getAsJsonObject();

            // todo complete this code... I'm doing rest of the thing assuming this is completed
            // todo anyhow these JSON related codes might get changed neh! one day we might be using RDB

        }
        return templates;
    }
}
