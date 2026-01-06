package com.javaaidev.adk.callback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;

public class JsonUtils {

  public static final ObjectMapper DEFAULT_MAPPER =
      new ObjectMapper().findAndRegisterModules().enable(SerializationFeature.INDENT_OUTPUT);

  public static Object fromJson(String json, Class<?> javaType) throws JsonProcessingException {
    return DEFAULT_MAPPER.readValue(cleanJson(json), javaType);
  }

  public static Object fromValue(Object value, Class<?> javaType) {
    return DEFAULT_MAPPER.convertValue(value, javaType);
  }

  public static String toJson(Object value) {
    if (value == null) {
      return "{}";
    }
    try {
      return DEFAULT_MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return value.toString();
    }
  }

  public static String cleanJson(String json) {
    if (StringUtils.isBlank(json)) {
      return json;
    }
    return json.replaceAll("(?s)```json\\s*", "").replaceAll("(?s)```", "").trim();
  }
}
