package org.apache.airavata.helix.impl.task.parsing;

import java.util.HashMap;
import java.util.Map;

public class ParserDAGElement {
    private String parentParser; //returns the id
    private String childParser; // returns the id
    private Map<String, String> outputInputMapping = new HashMap<>(); // assume that </usr/lahiruj/opensource.txt, airavata.txt> todo not the path in input just the file name
    //parent output path is mapped to child's input path
    //todo why is this necessary because only need the names of the files (output files) which can be set as the inputs to
    // todo the child parser's inputs. In the docker command it is only required to put names then we can go with the necessary
    // todo output files of the parent parser
    // multiple outputs and multiple inputs

    //todo this mapping more specifically output file to input file mapping
    //todo because a user cannot change the docker occasionally therefore parent parser's output file should be matched with
    // todo child parser's input files
    // todo eg. parent outputs --> opensource.txt, sports.txt, food.txt
    // todo child inputs --> airavata.txt, rugby.txt, pizza.txt
    // todo then user should know which output should be matched with which input


    public String getParentParser() {
        return parentParser;
    }

    public void setParentParser(String parentParser) {
        this.parentParser = parentParser;
    }

    public String getChildParser() {
        return childParser;
    }

    public void setChildParser(String childParser) {
        this.childParser = childParser;
    }

    public Map<String, String> getOutputInputMapping() {
        return outputInputMapping;
    }

    public void setOutputInputMapping(Map<String, String> outputInputMapping) {
        this.outputInputMapping = outputInputMapping;
    }
}
