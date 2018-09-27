package org.apache.airavata.helix.impl.task.parsing;

import com.google.gson.Gson;

public class ParserUtil {

    public static <T> T getTFromJsonStr(String strT, Class<T> classType) {
        return new Gson().fromJson(strT, classType);
    }

    public static <T> String getStrFromT(T obj) {
        return new Gson().toJson(obj);
    }

}
