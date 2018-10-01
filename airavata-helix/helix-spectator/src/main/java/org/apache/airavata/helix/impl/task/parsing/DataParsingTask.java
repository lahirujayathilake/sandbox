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

import org.apache.airavata.agents.api.AgentException;
import org.apache.airavata.agents.api.StorageResourceAdaptor;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.helix.core.AbstractTask;
import org.apache.airavata.helix.impl.task.TaskOnFailException;
import org.apache.airavata.helix.task.api.TaskHelper;
import org.apache.airavata.helix.task.api.annotation.TaskDef;
import org.apache.airavata.helix.task.api.annotation.TaskParam;
import org.apache.airavata.helix.task.api.support.AdaptorSupport;
import org.apache.airavata.model.data.movement.DataMovementProtocol;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Pick the input file named {@link ParserInfo#}
 * from the {@link #} directory and handover to
 * the {@link ParserInfo#containerName} Docker container to
 * get the desired {@link ParserInfo#mandatoryOutputFiles} and
 * {@link ParserInfo#optionalOutputFiles} output file
 *
 * @since 1.0.0-SNAPSHOT
 */
@TaskDef(name = "Data Parsing Task")
public class DataParsingTask extends AbstractTask {

    private final static Logger logger = LoggerFactory.getLogger(DataParsingTask.class);
    private String localWorkingDir;

    @TaskParam(name = "JSON String ParserInfo")
    private String jsonStrParserInfo;

    @TaskParam(name = "JSON String Parser DAG Element")
    private String jsonStrParserDagElement;

    @TaskParam(name = "Parsing Template ID")
    private String parsingTemplateId;

    @TaskParam(name = "Storage Input File Path")
    private String storageInputFilePath;

    @TaskParam(name = "Gateway ID")
    private String gatewayId;

    @TaskParam(name = "Storage Resource ID")
    private String storageResourceId;

    @TaskParam(name = "Data Movement Protocol")
    private String dataMovementProtocol;

    @TaskParam(name = "Storage Resource Credential Token")
    private String storageResourceCredToken;

    @TaskParam(name = "Storage Resource Login User Name")
    private String storageResourceLoginUName;

    @Override
    public TaskResult onRun(TaskHelper helper) {
        logger.info("Starting data parsing task " + getTaskId());

        try {
            // In this DAG element ChildParser is the current parser, only need parent parser's output to map child parser's input
            ParserDAGElement dagElement = ParserUtil.getObjFromJSONStr(jsonStrParserDagElement, ParserDAGElement.class);
            ParserInfo parserInfo = ParserUtil.getObjFromJSONStr(jsonStrParserInfo, ParserInfo.class);
            localWorkingDir = createLocalWorkingDir(parserInfo.getId());
            // Fetch and validate storage adaptor
            StorageResourceAdaptor storageResourceAdaptor = getStorageAdaptor(helper.getAdaptorSupport());

            //todo only required the parent - child output mapping --> this task is the child
            // todo should download the files from the storage resource then go for the following condition
            for (String sourceFile : dagElement.getInputOutputMapping().keySet()) {
                URI sourceURI = null;
                try {
                    // Even for the first parser in the DAG there should be an input to output mapping
                    sourceURI = new URI(storageInputFilePath + File.separator + dagElement.getInputOutputMapping().get(sourceFile));
                    logger.info("Downloading input file " + sourceURI.getPath() + " to the local path " + localWorkingDir);
                    storageResourceAdaptor.downloadFile(sourceURI.getPath(), localWorkingDir + File.separator + sourceFile);
                    logger.info("Input file downloaded to " + localWorkingDir);

                } catch (URISyntaxException e) {
                    throw new TaskOnFailException("Failed to obtain source URI for Data Parsing Task " + getTaskId(), true, e);

                } catch (AgentException e) {
                    throw new TaskOnFailException("Failed downloading input file " + sourceURI.getPath() + " to the local path " + localWorkingDir, false, e);
                }
            }

            ContainerStatus containerStatus = ContainerStatus.REMOVED;

            // Check whether the container is running if found stop the container
            Process procActive = Runtime.getRuntime().exec("docker ps -q -f name=" + parserInfo.getContainerName());
            try (InputStreamReader isr = new InputStreamReader(procActive.getInputStream())) {
                if (isr.read() != -1) {
                    containerStatus = ContainerStatus.ACTIVE;
                    logger.info("Docker container: " + parserInfo.getContainerName() +
                            " is active in data parsing task: " + getTaskId());

                    // Stop the container
                    Process procStop = Runtime.getRuntime().exec("docker stop " + parserInfo.getContainerName());
                    try (InputStreamReader isrStop = new InputStreamReader(procStop.getInputStream())) {
                        if (isrStop.read() != -1) {
                            containerStatus = ContainerStatus.INACTIVE;
                            logger.info("Stopped the Docker container: " + parserInfo.getContainerName() +
                                    " for data parsing task: " + getTaskId());
                        }
                    }
                }
            }

            // Check for an exited container if found remove it
            Process procExited = Runtime.getRuntime().exec("docker ps -aq -f status=exited -f name=" + parserInfo.getContainerName());
            try (InputStreamReader isr = new InputStreamReader(procExited.getInputStream())) {
                if (isr.read() != -1) {

                    Process procRemoved = Runtime.getRuntime().exec("docker rm " + parserInfo.getContainerName());
                    try (InputStreamReader isrRemoved = new InputStreamReader(procRemoved.getInputStream())) {
                        if (isrRemoved.read() != -1) {
                            containerStatus = ContainerStatus.REMOVED;
                            logger.info("Removed the exited Docker container: " + parserInfo.getContainerName() +
                                    " for data parsing task: " + getTaskId());
                        } else {
                            containerStatus = ContainerStatus.INACTIVE;
                        }
                    }
                }
            }

            if (containerStatus == ContainerStatus.REMOVED) {
                /*
                 * Example command
                 *
                 *      "docker run --name CONTAINER-lahiruj/gaussian " +
                 *      "-it --rm=true " +
                 *      "--security-opt seccomp=/path/to/seccomp/profile.json " +
                 *      "--label com.example.foo=bar " +
                 *      "--env LD_LIBRARY_PATH=/usr/local/lib " +
                 *      "-v /Users/lahiruj/tmp/dir:/datacat/working-dir " +     // local directory is mounted in read-write mode
                 *      "lahiruj/gaussian " +                                   // docker image name
                 *      "python " +                                             // programming language
                 *      "gaussian.py " +                                        // file to be executed
                 *      "input-gaussian.json "                                   // input file
                 *      "output.json"                                           // output file path
                 *
                 */

//                String dockerCommand = "docker run " +
//                        "--name " + parserInfo.getContainerName() +
//                        " -t " +
//                        parserInfo.getAutomaticallyRmContainer() + " " +
//                        parserInfo.getRunInDetachedMode() + " " +
//                        parserInfo.getSecurityOpt() + " " +
//                        parserInfo.getLabel() + " " +
//                        parserInfo.getEnvVariables() + " " +
//                        parserInfo.getCpus() + " " +
//                        " -v " + parserInputDir + ":" + parserInfo.getDockerWorkingDirPath() + " " +
//                        parserInfo.getDockerImageName() + " " +
//                        parserInfo.getExecutableBinary() + " " +
//                        parserInfo.getExecutingFile() + " ";

                StringBuilder dockerCommand = new StringBuilder("docker run --name ")
                        .append(parserInfo.getContainerName())
                        .append(" -t")
                        .append(parserInfo.getAutomaticallyRmContainer()).append(" ")
                        .append(parserInfo.getRunInDetachedMode()).append(" ")
                        .append(parserInfo.getSecurityOpt()).append(" ")
                        .append(parserInfo.getLabel()).append(" ")
                        .append(parserInfo.getEnvVariables()).append(" ")
                        .append(parserInfo.getCpus())
                        .append(" -v ")
                        .append(localWorkingDir)
                        .append(":")
                        .append(parserInfo.getDockerWorkingDirPath()).append(" ")
                        .append(parserInfo.getDockerImageName()).append(" ")
                        .append(parserInfo.getExecutableBinary()).append(" ")
                        .append(parserInfo.getExecutingFile()).append(" ");

                // Appending input files
                parserInfo.getInputFiles().forEach(f -> dockerCommand.append(f).append(" "));

                // Appending output file names FIXME I don't think this is necessary because now there is a mapping only the input files are needed
                parserInfo.getMandatoryOutputFiles().forEach(f -> dockerCommand.append(f).append(" "));
                parserInfo.getOptionalOutputFiles().forEach(f -> dockerCommand.append(f).append(" "));

                // Run the docker command
                Process proc = Runtime.getRuntime().exec(dockerCommand.toString().trim());
                try (BufferedReader stdError = new BufferedReader(new InputStreamReader(proc.getErrorStream()))) {
                    String line;
                    StringBuilder error = new StringBuilder();

                    // read errors from the attempted command
                    while ((line = stdError.readLine()) != null) {
                        error.append(line);
                    }

                    if (error.length() > 0) {
                        logger.error("Error running Docker command " + error + " for task " + getTaskId());
                        throw new TaskOnFailException("Could not run Docker command successfully for task " + getTaskId(), true, null);
                    }
                }

                String uploadPath = File.separator + "data-parsing" + File.separator + parsingTemplateId +
                        File.separator + parserInfo.getId() + File.separator + "outputs" + File.separator;

                // Validate whether the mandatory output files are created and upload them to storage resource
                for (String mandatoryFile : parserInfo.getMandatoryOutputFiles()) {
                    File f = new File(localWorkingDir + mandatoryFile);
                    if (f.exists()) {
                        // Uploading output file to the storage resource
                        try {
                            logger.info("Uploading the output file " + mandatoryFile + " to " + uploadPath + " from local path " + localWorkingDir);
                            storageResourceAdaptor.uploadFile(localWorkingDir + mandatoryFile, uploadPath + mandatoryFile);
                            logger.info("Output file successfully uploaded to " + uploadPath);

                        } catch (AgentException e) {
                            throw new TaskOnFailException("Failed uploading the output file to " + uploadPath + " from local path " + localWorkingDir, false, e);
                        }

                    } else {
                        throw new TaskOnFailException("File: " + mandatoryFile + " has not been successfully created running the task " + getTaskId(), false, null);
                    }
                }

                // Upload optional files
                for (String optionalFile : parserInfo.getOptionalOutputFiles()) {
                    File f = new File(localWorkingDir + optionalFile);
                    if (f.exists()) {
                        // Uploading the optional output file to the storage resource
                        try {
                            logger.info("Uploading the optional output file " + optionalFile + " to " + uploadPath + " from local path " + localWorkingDir);
                            storageResourceAdaptor.uploadFile(localWorkingDir + optionalFile, uploadPath + optionalFile);
                            logger.info("Optional output file successfully uploaded to " + uploadPath);

                        } catch (AgentException e) {
                            logger.warn("Failed uploading the output file to " + uploadPath + " from local path " + localWorkingDir);
                        }
                    }
                }

                return onSuccess("Data parsing task " + getTaskId() + " successfully completed");

            } else {
                throw new TaskOnFailException("Docker container has not been successfully " +
                        (containerStatus == ContainerStatus.ACTIVE ? "stopped " : "removed ") +
                        "for data parsing task" + getTaskId(), true, null);
            }

        } catch (TaskOnFailException e) {
            if (e.getError() != null) {
                logger.error(e.getReason(), e.getError());
            } else {
                logger.error(e.getReason());
            }

            return onFail(e.getReason(), e.isCritical());

        } catch (Exception e) {
            logger.error("Unknown error while executing data parsing task " + getTaskId(), e);
            return onFail("Unknown error while executing data parsing task " + getTaskId(), false);

        } finally {
            deleteLocalWorkingDir(localWorkingDir);
        }
    }

    @Override
    public void onCancel() {

    }

    private String createLocalWorkingDir(String parserId) throws TaskOnFailException {
        String localDir = ServerSettings.getLocalDataLocation();
        localDir = (localDir.endsWith(File.separator) ? localDir : localDir + File.separator) +
                "parsers" + File.separator + parserId + File.separator + "data" + File.separator;
        try {
            FileUtils.forceMkdir(new File(localDir));
            return localDir;

        } catch (IOException e) {
            throw new TaskOnFailException("Failed build directories " + localDir, true, e);
        }
    }

    private void deleteLocalWorkingDir(String directory) {
        try {
            FileUtils.deleteDirectory(new File(directory));
        } catch (IOException e) {
            logger.warn("Failed to delete the local working directory: " + directory);
        }
    }

    private StorageResourceAdaptor getStorageAdaptor(AdaptorSupport adaptorSupport) throws TaskOnFailException {
        try {
            StorageResourceAdaptor storageResourceAdaptor = adaptorSupport.fetchStorageAdaptor(
                    gatewayId,
                    storageResourceId,
                    ParserUtil.getObjFromJSONStr(dataMovementProtocol, DataMovementProtocol.class),
                    storageResourceCredToken,
                    storageResourceLoginUName);

            if (storageResourceAdaptor == null) {
                throw new TaskOnFailException("Storage resource adaptor for " + storageResourceId + " cannot be null",
                        true, null);
            }
            return storageResourceAdaptor;

        } catch (AgentException e) {
            throw new TaskOnFailException("Failed to obtain adaptor for storage resource " + storageResourceId +
                    " in task " + getTaskId(), true, e);
        }
    }

    private enum ContainerStatus {
        ACTIVE,
        INACTIVE,
        REMOVED
    }
}
