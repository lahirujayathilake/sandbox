/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.airavata.helix.impl.task.parsing.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParserYamlConfiguration {

    private static final String PARSER_PROPERTY_FILE = "conf/datacat-parser.yaml"; //TODO I changed this so no need the below line of code
    //    private static final String DEFAULT_PARSER_PROPERTY_FILE = "conf/datacat-parser.yaml";
    private static final String DATA_PARSERS = "data.parsers";
    private static final String PARSERS = "parsers";
    private static final String DATA_TYPE = "data.type";
    private static final String DATA_DETECTOR = "data.detector";

    private List<ParserConfig> dataParsers = new ArrayList<>();

    public ParserYamlConfiguration() throws Exception {
        InputStream resourceAsStream = ClassLoader.getSystemResource(PARSER_PROPERTY_FILE).openStream();
        parse(resourceAsStream);
    }

    private void parse(InputStream resourceAsStream) throws Exception {
        if (resourceAsStream == null) {
            throw new Exception("Configuration file{datacat-parser.yaml} is not fund");
        }

        Object load = new Yaml().load(resourceAsStream);
        if (load == null) {
            throw new Exception("Yaml configuration object is null");
        }

        if (load instanceof Map) {
            Map<String, Object> loadMap = (Map<String, Object>) load;
            List<Map<String, Object>> parserYamls = (List<Map<String, Object>>) loadMap.get(DATA_PARSERS);

            if (parserYamls != null) {

                ParserConfig parserConfig;
                for (Map<String, Object> parser : parserYamls) {
                    parserConfig = new ParserConfig();
                    parserConfig.setDataType((String) parser.get(DATA_TYPE));
                    parserConfig.setDataDetectorClass((String) parser.get(DATA_DETECTOR));
                    parserConfig.setParserClasses((ArrayList<String>) parser.get(PARSERS));
                    dataParsers.add(parserConfig);
                }
            }
        }
    }

    public List<ParserConfig> getDataParsers() {
        return dataParsers;
    }
}
