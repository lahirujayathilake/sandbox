package org.apache.airavata.helix.impl.task.parsing;

import com.google.gson.Gson;

public class ParserUtil {

    public static <T> T getObjFromJSONStr(String strT, Class<T> classType) {
        return new Gson().fromJson(strT, classType);
    }

    public static <T> String getStrFromObj(T obj) {
        return new Gson().toJson(obj);
    }

}
