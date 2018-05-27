package org.apache.airavata.helix.impl.task.parsing;

import org.apache.airavata.helix.impl.task.parsing.commons.CatalogFileRequest;

import java.util.List;

public interface IParserResolver {

    List<IParser> resolveParser(String localDirPath, CatalogFileRequest catalogFileRequest) throws Exception;

}
