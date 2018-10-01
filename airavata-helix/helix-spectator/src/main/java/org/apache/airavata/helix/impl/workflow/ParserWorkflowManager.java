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
package org.apache.airavata.helix.impl.workflow;

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.helix.core.AbstractTask;
import org.apache.airavata.helix.core.OutPort;
import org.apache.airavata.helix.core.util.MonitoringUtil;
import org.apache.airavata.helix.impl.task.parsing.CatalogUtil;
import org.apache.airavata.helix.impl.task.parsing.DataParsingTask;
import org.apache.airavata.helix.impl.task.parsing.ParserInfo;
import org.apache.airavata.model.appcatalog.appinterface.ApplicationInterfaceDescription;
import org.apache.airavata.model.process.ProcessModel;
import org.apache.airavata.monitor.JobStatusResult;
import org.apache.airavata.monitor.kafka.JobStatusResultDeserializer;
import org.apache.airavata.registry.api.RegistryService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Workflow Manager which will create and launch a Data Parsing DAG
 *
 * @since 1.0.0-SNAPSHOT
 */
public class ParserWorkflowManager extends WorkflowManager {

    private final static Logger logger = LoggerFactory.getLogger(ParserWorkflowManager.class);

    public ParserWorkflowManager() throws ApplicationSettingsException {
        super(ServerSettings.getSetting("parser.workflow.manager.name"));
    }

    private void init() throws Exception {
        super.initComponents();
    }

    private boolean process(JobStatusResult jobStatusResult) {
        try {
            if (MonitoringUtil.hasMonitoringRegistered(getCuratorClient(), jobStatusResult.getJobId())) {

                String processId = Optional.ofNullable(MonitoringUtil.getProcessIdByJobId(getCuratorClient(), jobStatusResult.getJobId()))
                        .orElseThrow(() -> new Exception("Can not find the process for job id " + jobStatusResult.getJobId()));

                String experimentId = Optional.ofNullable(MonitoringUtil.getExperimentIdByJobId(getCuratorClient(), jobStatusResult.getJobId()))
                        .orElseThrow(() -> new Exception("Can not find the experiment for job id " + jobStatusResult.getJobId()));

                RegistryService.Client registryClient = getRegistryClientPool().getResource();
                ProcessModel processModel;
                ApplicationInterfaceDescription appDescription;
                try {
                    processModel = registryClient.getProcess(processId);
                    appDescription = registryClient.getApplicationInterface(processModel.getApplicationInterfaceId());
                    getRegistryClientPool().returnResource(registryClient);

                } catch (Exception e) {
                    logger.error("Failed to fetch process or application description from registry associated with process id " + processId, e);
                    getRegistryClientPool().returnResource(registryClient);
                    throw new Exception("Failed to fetch process or application description from registry associated with process id " + processId, e);
                }
                //todo parser template --> dag -- > parser info --> dag
                DAGCatalogEntry dagCatalogEntry = CatalogUtil.dagCatalogLookup(ServerSettings.getSetting("parser.dag.catalog.path"))
                        .stream()
                        .filter(s -> s.getApplicationName().equals(appDescription.getApplicationName()))
                        .findFirst()
                        .orElseThrow(() -> new Exception("Can not find the DAG catalog entry for the experiment: " + experimentId));

                File inputFile = new File(dagCatalogEntry.getLocalWorkingDir().endsWith(File.separator)
                        ? dagCatalogEntry.getLocalWorkingDir() + dagCatalogEntry.getInputFileName()
                        : dagCatalogEntry.getLocalWorkingDir() + File.separator + dagCatalogEntry.getInputFileName());

                if (!inputFile.exists()) {
                    logger.error("Input file: " + inputFile.getName() + " does not exists");
                    return false;
                }

                final List<AbstractTask> allTasks = new ArrayList<>();
                List<ParserInfo> catalogEntries = fetchEntries(dagCatalogEntry);

                // Set the input file name into the first ParserInfo
                catalogEntries.get(0).setInputFileName(dagCatalogEntry.getInputFileName());
                // Set the output file name into the last ParserInfo
                catalogEntries.get(catalogEntries.size() - 1).setOutputFileName(dagCatalogEntry.getOutputFileName());

                for (int i = 0; i < catalogEntries.size(); i++) {
                    ParserInfo entry = catalogEntries.get(i);
                    if (i > 0) {
                        // Set the input file name as the previous catalog entry output file name
                        entry.setInputFileName(catalogEntries.get(i - 1).getOutputFileName());
                    }

                    DataParsingTask task = new DataParsingTask();
                    task.setJsonStrCatalogEntry(CatalogUtil.catalogEntryToJSONString(entry));
                    task.setLocalWorkingDir(dagCatalogEntry.getLocalWorkingDir());
                    task.setTaskId("ID-" + entry.getDockerImageName().replaceAll("[^a-zA-Z0-9_.-]", "-"));

                    if (allTasks.size() > 0) {
                        allTasks.get(allTasks.size() - 1).setNextTask(new OutPort(task.getTaskId(), task));
                    }
                    allTasks.add(task);
                    logger.info("Successfully added the data parsing task: " + task.getTaskId() + " to the task DAG");
                }

                String workflowName = getWorkflowOperator()
                        .launchWorkflow(processId + "-DataParsing-" + UUID.randomUUID().toString(),
                                new ArrayList<>(allTasks), true, false);
                try {
                    MonitoringUtil.registerWorkflow(getCuratorClient(), processId, workflowName);

                } catch (Exception e) {
                    logger.error("Failed to save workflow " + workflowName + " of process " + processId +
                            " in zookeeper registry. " + "This will affect cancellation tasks", e);
                }
                return true;

            } else {
                logger.warn("Could not find a monitoring registered for the job id " + jobStatusResult.getJobId());
                return false;
            }

        } catch (Exception e) {
            logger.error("Failed to create the DataParsing task DAG", e);
            return false;
        }
    }

    private void runConsumer() throws ApplicationSettingsException {
        final Properties props = new Properties();
        final Consumer<String, JobStatusResult> consumer = new KafkaConsumer<>(props);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ServerSettings.getSetting("kafka.broker.url"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ServerSettings.getSetting("kafka.broker.consumer.group"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JobStatusResultDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer.subscribe(Collections.singletonList(ServerSettings.getSetting("kafka.broker.topic"))); // todo change the topic name  eg. kafka.broker.parser.topic

        while (true) {
            final ConsumerRecords<String, JobStatusResult> consumerRecords = consumer.poll(Long.MAX_VALUE);

            for (TopicPartition partition : consumerRecords.partitions()) {
                List<ConsumerRecord<String, JobStatusResult>> partitionRecords = consumerRecords.records(partition);
                for (ConsumerRecord<String, JobStatusResult> record : partitionRecords) {
                    boolean success = process(record.value());
                    logger.info("Status of processing job: " + record.value().getJobId() + " : " + success);
                    if (success) {
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(record.offset() + 1)));
                    }
                }
            }

            consumerRecords.forEach(record -> process(record.value()));
            consumer.commitAsync();
        }
    }

    private List<ParserInfo> fetchEntries(DAGCatalogEntry dagEntry) throws Exception {
        List<ParserInfo> entries = new ArrayList<>();
        List<ParserInfo> catalog = CatalogUtil.catalogLookup(ServerSettings.getSetting("parser.catalog.path"));
        for (String dockerImageName : dagEntry.getTaskDag()) {
            ParserInfo e = catalog.stream().filter(s -> s.getDockerImageName().equals(dockerImageName))
                    .findFirst()
                    .orElseThrow(() -> new Exception("Cannot find the Dockerized container for application: " + dagEntry.getApplicationName()));

            entries.add(e);
        }
        return entries;
    }

    private URI getStorageOutputURI(String parserId) {
        try {
            return new URI(File.separator + "parsers" + File.separator + parsingTemplateId + File.separator +
                    parserId + "outputs" + File.separator);

        } catch (URISyntaxException e) {
            throw new Exception("Failed to extract the output path of parser: " + parserId +
                    " in parsing template: " + parsingTemplateId, true, null);
        }
    }

    public static void main(String[] args) throws Exception {
        ParserWorkflowManager manager = new ParserWorkflowManager();
        manager.init();
        manager.runConsumer();
    }
}
