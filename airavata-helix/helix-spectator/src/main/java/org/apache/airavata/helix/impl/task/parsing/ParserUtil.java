package org.apache.airavata.helix.impl.task.parsing;

import com.google.gson.Gson;

import java.io.File;

public class ParserUtil {

    public static <T> T getObjFromJSONStr(String strT, Class<T> classType) {
        return new Gson().fromJson(strT, classType);
    }

    public static <T> String getStrFromObj(T obj) {
        return new Gson().toJson(obj);
    }

    public static String getStorageUploadPath(String parsingTemplateId, String parserId) {
        return File.separator + "data-parsing" + File.separator + parsingTemplateId + File.separator + parserId +
                File.separator + "outputs" + File.separator;
    }

}
