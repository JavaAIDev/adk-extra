package com.javaaidev.adk.callback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.adk.agents.CallbackContext;
import com.google.adk.agents.Callbacks.AfterAgentCallback;
import com.google.genai.types.Content;
import io.reactivex.rxjava3.core.Maybe;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert context state to a different type, only supports JSON string and Map
 *
 * @param sourceContextKey Context key of the source
 * @param targetJavaType Java type of the target
 * @param targetContextKey Context key of the target
 * @param escalateOnConversionError Should escalate when conversion failed
 */
public record ContextStateConversionAfterAgentCallback(
    String sourceContextKey,
    Class<?> targetJavaType,
    String targetContextKey,
    boolean escalateOnConversionError)
    implements AfterAgentCallback {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ContextStateConversionAfterAgentCallback.class);

  @Override
  public Maybe<Content> call(CallbackContext callbackContext) {
    var source = callbackContext.state().get(sourceContextKey);
    try {
      Object result = null;
      if (source instanceof String json) {
        result = JsonUtils.fromJson(json, targetJavaType);
      } else if (source instanceof Map<?, ?> map) {
        result = JsonUtils.fromValue(map, targetJavaType);
      }
      if (result != null) {
        callbackContext.state().put(targetContextKey, result);
      }
    } catch (JsonProcessingException | IllegalArgumentException e) {
      LOGGER.error("Failed to convert state key {} to type {}", sourceContextKey, targetJavaType);
      if (escalateOnConversionError) {
        callbackContext.eventActions().setEscalate(true);
      }
    }
    return Maybe.empty();
  }
}
