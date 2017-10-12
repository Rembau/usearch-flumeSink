package com.unilife.search.sink;

import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by rembau on 2017/3/13.
 */
public class GsonESUtil {
    private final static Logger logger = LoggerFactory.getLogger(GsonESUtil.class);

    private static Gson gson;
    static {
        final GsonBuilder gsonBuilder = new GsonBuilder();

        final DateFormat iso8601Format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.US);
        iso8601Format.setTimeZone(TimeZone.getTimeZone("UTC"));

        final DateFormat simpleDateFormat = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        gsonBuilder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {

            public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
                String asString = null;
                try {
                    asString = json.getAsJsonPrimitive().getAsString();
                    return iso8601Format.parse(asString);
                } catch (ParseException e) {
                    logger.error("时间字符串解析失败：" + json + "，时间字符串：" + asString, e);
                }
                return null;
            }
        });

        gsonBuilder.registerTypeAdapter(Date.class, new JsonSerializer<Date>(){
            @Override
            public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context) {
                String format = simpleDateFormat.format(src);
                return new JsonPrimitive(format);
            }
        });

        gson = gsonBuilder.create();
    }


    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> tClass) {
        return gson.fromJson(json, tClass);
    }
}
