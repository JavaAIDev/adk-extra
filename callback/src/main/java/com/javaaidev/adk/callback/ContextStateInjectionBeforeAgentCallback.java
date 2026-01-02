package com.javaaidev.adk.callback;

import com.google.adk.agents.CallbackContext;
import com.google.adk.agents.Callbacks.BeforeAgentCallback;
import com.google.genai.types.Content;
import io.reactivex.rxjava3.core.Maybe;
import java.util.Map;
import java.util.Objects;

/** Inject context state before running an agent */
public class ContextStateInjectionBeforeAgentCallback implements BeforeAgentCallback {

  private final Maybe<Map<String, Object>> stateDelta;

  public ContextStateInjectionBeforeAgentCallback(Maybe<Map<String, Object>> stateDelta) {
    this.stateDelta = Objects.requireNonNull(stateDelta, "State delta cannot be null");
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
