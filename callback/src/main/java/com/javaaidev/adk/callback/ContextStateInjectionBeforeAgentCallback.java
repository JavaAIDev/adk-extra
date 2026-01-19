package com.javaaidev.adk.callback;

import com.google.adk.agents.CallbackContext;
import com.google.adk.agents.Callbacks.BeforeAgentCallback;
import com.google.genai.types.Content;
import io.reactivex.rxjava3.core.Maybe;
import java.util.Map;
import java.util.Objects;

/**
 * Inject context state before running an agent
 *
 * @param stateDelta Context state to inject
 */
public record ContextStateInjectionBeforeAgentCallback(Maybe<Map<String, Object>> stateDelta)
    implements BeforeAgentCallback {

  public ContextStateInjectionBeforeAgentCallback(Map<String, Object> stateDelta) {
    this(Maybe.just(Objects.requireNonNull(stateDelta, "stateDelta cannot be null")));
  }

  @Override
  public Maybe<Content> call(CallbackContext callbackContext) {
    return stateDelta
        .filter(state -> state != null && !state.isEmpty())
        .doOnSuccess(state -> callbackContext.state().putAll(state))
        .ignoreElement()
        .andThen(Maybe.empty());
  }
}
