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

import org.apache.airavata.helix.impl.task.AiravataTask;
import org.apache.airavata.helix.impl.task.TaskContext;
import org.apache.airavata.helix.impl.task.parsing.commons.CatalogFileRequest;
import org.apache.airavata.helix.impl.task.parsing.util.DataCatConstants;
import org.apache.airavata.helix.impl.task.parsing.util.FileHelper;
import org.apache.airavata.helix.impl.task.parsing.util.WorkerProperties;
import org.apache.airavata.helix.task.api.TaskHelper;
import org.apache.airavata.helix.task.api.annotation.TaskDef;
import org.apache.airavata.model.status.ProcessState;
import org.apache.helix.task.TaskResult;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

@TaskDef(name = "Data Parsing Task")
public class DataParsingTask extends AiravataTask {

    private final static Logger logger = LoggerFactory.getLogger(DataParsingTask.class);

    @Override
    public TaskResult onRun(TaskHelper helper, TaskContext taskContext) {
        logger.info("Starting data parsing task " + getTaskId() + " in experiment " + getExperimentId());
        saveAndPublishProcessStatus(ProcessState.DATA_PARSING);

        /****************************************************************************************************/

        try {
            //FIXME for the time being a dumb catalogFileRequest is created, this should be changed
            /****************************************************************/
            CatalogFileRequest catalogFileRequest = new CatalogFileRequest();
            /****************************************************************/

            String workingDir = null;
            String localDirPath = null;
            URI uri = new URI(catalogFileRequest.getDirUri());
            IParserResolver parserResolver = instantiate(
                    WorkerProperties.getInstance().getProperty(DataCatConstants.PARSER_RESOLVER_CLASS, ""),
                    IParserResolver.class);

            //Copying data to the local directory
            if (!uri.getScheme().contains("file")) {
                workingDir = WorkerProperties.getInstance().getProperty(DataCatConstants.WORKING_DIR, "/tmp");
                localDirPath = new FileHelper().createLocalCopyOfDir(uri, workingDir);
            } else {
                localDirPath = uri.getPath();
            }

            logger.info("Parsing file " + localDirPath);
            List<IParser> parsers = parserResolver.resolveParser(localDirPath, catalogFileRequest);
            if (parsers != null && parsers.size() > 0) {
                JSONObject jsonObject = new JSONObject();
                for (IParser parser : parsers) {
                    jsonObject = parser.parse(jsonObject, localDirPath, catalogFileRequest.getIngestMetadata());
                }
//            registry.create(jsonObject); //TODO this only creates the last parsed JSON output, and no need to persist in this case
                logger.info("Published data for directory : " + catalogFileRequest.getDirUri());
            } else {
                logger.warn("No suitable parser found for directory : " + catalogFileRequest.getDirUri());
            }

        } catch (URISyntaxException e) {
            e.printStackTrace();

        } catch (IOException e) {
            e.printStackTrace();

        } catch (Exception e) {
            e.printStackTrace();
        }

        /****************************************************************************************************/
    }

    @Override
    public void onCancel(TaskContext taskContext) {

    }

    private <T> T instantiate(final String className, final Class<T> type) {
        try {
            return type.cast(Class.forName(className).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

}
