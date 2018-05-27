package org.apache.airavata.helix.impl.task.parsing;

import org.apache.airavata.helix.impl.task.parsing.commons.CatalogFileRequest;

public interface IDataDetector {
    boolean detectData(String localDirPath, CatalogFileRequest catalogFileRequest);
}
