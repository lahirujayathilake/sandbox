package org.apache.airavata.helix.impl.task.parsing;

import java.util.List;

public class ParsingTemplate {
    private String id;
    private String applicationInterface;
    private String initialInputsPath;
    private List<ParserDAGElement> parserDag;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getApplicationInterface() {
        return applicationInterface;
    }

    public void setApplicationInterface(String applicationInterface) {
        this.applicationInterface = applicationInterface;
    }

    public String getInitialInputsPath() {
        return initialInputsPath;
    }

    public void setInitialInputsPath(String initialInputsPath) {
        this.initialInputsPath = initialInputsPath;
    }

    public List<ParserDAGElement> getParserDag() {
        return parserDag;
    }

    public void setParserDag(List<ParserDAGElement> parserDag) {
        this.parserDag = parserDag;
    }
}
