package org.apache.airavata.helix.impl.task.parsing.util;

import org.apache.airavata.helix.impl.task.TaskOnFailException;
import org.apache.airavata.helix.impl.task.parsing.IDataDetector;
import org.apache.airavata.helix.impl.task.parsing.IParser;
import org.apache.airavata.helix.impl.task.parsing.commons.ApplicationTypes;
import org.apache.airavata.helix.impl.task.parsing.commons.CatalogFileRequest;
import org.apache.airavata.helix.impl.task.parsing.config.ParserConfig;
import org.apache.airavata.helix.impl.task.parsing.config.ParserYamlConfiguration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ParserResolver {

    public List<IParser> resolveParser(String localDirPath, CatalogFileRequest catalogFileRequest) throws TaskOnFailException {
        List<IParser> parsers = new ArrayList<>();
        String mimeType = catalogFileRequest.getMimeType();

        if (mimeType == null || mimeType.isEmpty()) {
            mimeType = resolveMimeTypeForData(localDirPath, catalogFileRequest);
        }

        assert mimeType != null;
        if (mimeType.equals(ApplicationTypes.APPLICATION_GAUSSIAN)) {
            buildParsers(parsers, mimeType);

        } else if (mimeType.equals(ApplicationTypes.APPLICATION_GAMESS)) {
            buildParsers(parsers, mimeType);

        } else if (mimeType.equals(ApplicationTypes.APPLICATION_MOLPRO)) {
            buildParsers(parsers, mimeType);

        } else if (mimeType.equals(ApplicationTypes.APPLICATION_NWCHEM)) {
            buildParsers(parsers, mimeType);
        }

        return parsers;
    }

    private String resolveMimeTypeForData(String localDirPath, CatalogFileRequest catalogFileRequest) throws TaskOnFailException {
        try {
            List<ParserConfig> parserConfigs = new ParserYamlConfiguration().getDataParsers();
            for (ParserConfig p : parserConfigs) {
                String detectClass = p.getDataDetectorClass();
                IDataDetector dataDetector = instantiate(detectClass, IDataDetector.class);
                if (dataDetector.detectData(localDirPath, catalogFileRequest)) {
                    return p.getDataType();
                }
            }
            return null;

        } catch (Exception e) {
            throw new TaskOnFailException("Failed when retrieving parsers from Yaml configuration", true, e);
        }
    }

    // TODO according to this article -> https://stackoverflow.com/questions/2215843/using-reflection-in-java-to-create-a-new-instance-with-the-reference-variable-ty?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
    // TODO this way of getting the class is ugly. Check this and identify whether this needs to be changed
    private <T> T instantiate(final String className, final Class<T> type) {
        try {
            return type.cast(Class.forName(className).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private void buildParsers(List<IParser> parsers, String applicationType) throws TaskOnFailException {
        try {
            Optional<ParserConfig> parserConfig = new ParserYamlConfiguration()
                    .getDataParsers().stream().filter(p -> p.getDataType().equals(applicationType)).findFirst();

            parserConfig.ifPresent(config -> config.getParserClasses().forEach(className -> {
                if (className != null && !className.isEmpty()) {
                    parsers.add(instantiate(className, IParser.class));
                }
            }));
        } catch (Exception e) {
            throw new TaskOnFailException("Failed when retrieving parsers from Yaml configuration", true, e);
        }
    }
}
