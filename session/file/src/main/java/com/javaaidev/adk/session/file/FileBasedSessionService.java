package com.javaaidev.adk.session.file;

import static java.util.stream.Collectors.toCollection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.sessions.BaseSessionService;
import com.google.adk.sessions.GetSessionConfig;
import com.google.adk.sessions.ListEventsResponse;
import com.google.adk.sessions.ListSessionsResponse;
import com.google.adk.sessions.Session;
import com.google.adk.sessions.State;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** File based implementation of {@linkplain BaseSessionService} */
public class FileBasedSessionService implements BaseSessionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedSessionService.class);

  private final ObjectMapper objectMapper =
      new ObjectMapper()
          .findAndRegisterModules()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, Session>>>
      sessions;
  private final ConcurrentMap<String, ConcurrentMap<String, ConcurrentMap<String, Object>>>
      userState;
  private final ConcurrentMap<String, ConcurrentMap<String, Object>> appState;
  private final Path root;

  public FileBasedSessionService(Path root) {
    Objects.requireNonNull(root, "root path cannot be null");
    if (!Files.exists(root)) {
      try {
        Files.createDirectories(root);
      } catch (IOException e) {
        LOGGER.error("Failed to create artifacts directory", e);
      }
    }
    this.root = root.normalize().toAbsolutePath();
    this.sessions = new ConcurrentHashMap<>();
    this.userState = new ConcurrentHashMap<>();
    this.appState = new ConcurrentHashMap<>();
    readAllSessions();
    readAppState();
    readUserState();
    LOGGER.info("Session data saved to {}", this.root);
  }

  @Override
  public Single<Session> createSession(
      String appName,
      String userId,
      @Nullable ConcurrentMap<String, Object> state,
      @Nullable String sessionId) {
    Objects.requireNonNull(appName, "appName cannot be null");
    Objects.requireNonNull(userId, "userId cannot be null");

    String resolvedSessionId =
        Optional.ofNullable(sessionId)
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .orElseGet(() -> UUID.randomUUID().toString());

    ConcurrentMap<String, Object> initialState =
        (state == null) ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(state);
    List<Event> initialEvents = new ArrayList<>();

    Session newSession =
        Session.builder(resolvedSessionId)
            .appName(appName)
            .userId(userId)
            .state(initialState)
            .events(initialEvents)
            .lastUpdateTime(Instant.now())
            .build();

    sessions
        .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(userId, k -> new ConcurrentHashMap<>())
        .put(resolvedSessionId, newSession);

    try {
      writeSession(appName, userId, resolvedSessionId, newSession);
    } catch (IOException e) {
      LOGGER.error("Failed to write session {}", resolvedSessionId, e);
    }

    Session returnCopy = copySession(newSession);
    return Single.just(mergeWithGlobalState(appName, userId, returnCopy));
  }

  @Override
  public Maybe<Session> getSession(
      String appName, String userId, String sessionId, Optional<GetSessionConfig> configOpt) {
    Objects.requireNonNull(appName, "appName cannot be null");
    Objects.requireNonNull(userId, "userId cannot be null");
    Objects.requireNonNull(sessionId, "sessionId cannot be null");
    Objects.requireNonNull(configOpt, "configOpt cannot be null");

    Session storedSession =
        sessions
            .getOrDefault(appName, new ConcurrentHashMap<>())
            .getOrDefault(userId, new ConcurrentHashMap<>())
            .get(sessionId);

    if (storedSession == null) {
      return Maybe.empty();
    }

    Session sessionCopy = copySession(storedSession);

    GetSessionConfig config = configOpt.orElse(GetSessionConfig.builder().build());
    List<Event> eventsInCopy = sessionCopy.events();

    config
        .numRecentEvents()
        .ifPresent(
            num -> {
              if (!eventsInCopy.isEmpty() && num < eventsInCopy.size()) {
                List<Event> eventsToRemove = eventsInCopy.subList(0, eventsInCopy.size() - num);
                eventsToRemove.clear();
              }
            });

    if (config.numRecentEvents().isEmpty() && config.afterTimestamp().isPresent()) {
      Instant threshold = config.afterTimestamp().get();

      eventsInCopy.removeIf(
          event -> getEventTimestampEpochSeconds(event) < threshold.getEpochSecond());
    }

    return Maybe.just(mergeWithGlobalState(appName, userId, sessionCopy));
  }

  private long getEventTimestampEpochSeconds(Event event) {
    return event.timestamp() / 1000L;
  }

  @Override
  public Single<ListSessionsResponse> listSessions(String appName, String userId) {
    Objects.requireNonNull(appName, "appName cannot be null");
    Objects.requireNonNull(userId, "userId cannot be null");

    Map<String, Session> userSessionsMap =
        sessions.getOrDefault(appName, new ConcurrentHashMap<>()).get(userId);

    if (userSessionsMap == null || userSessionsMap.isEmpty()) {
      return Single.just(ListSessionsResponse.builder().build());
    }

    List<Session> sessionCopies =
        userSessionsMap.values().stream()
            .map(this::copySessionMetadata)
            .collect(toCollection(ArrayList::new));

    return Single.just(ListSessionsResponse.builder().sessions(sessionCopies).build());
  }

  @Override
  public Completable deleteSession(String appName, String userId, String sessionId) {
    Objects.requireNonNull(appName, "appName cannot be null");
    Objects.requireNonNull(userId, "userId cannot be null");
    Objects.requireNonNull(sessionId, "sessionId cannot be null");

    ConcurrentMap<String, Session> userSessionsMap =
        sessions.getOrDefault(appName, new ConcurrentHashMap<>()).get(userId);

    if (userSessionsMap != null) {
      userSessionsMap.remove(sessionId);
    }

    var sessionPath = filePath(appName, userId, sessionId);
    try {
      Files.deleteIfExists(sessionPath);
    } catch (IOException e) {
      LOGGER.error("Failed to remove session {}", sessionId, e);
      return Completable.error(e);
    }
    return Completable.complete();
  }

  @Override
  public Single<ListEventsResponse> listEvents(String appName, String userId, String sessionId) {
    Objects.requireNonNull(appName, "appName cannot be null");
    Objects.requireNonNull(userId, "userId cannot be null");
    Objects.requireNonNull(sessionId, "sessionId cannot be null");

    Session storedSession =
        sessions
            .getOrDefault(appName, new ConcurrentHashMap<>())
            .getOrDefault(userId, new ConcurrentHashMap<>())
            .get(sessionId);

    if (storedSession == null) {
      return Single.just(ListEventsResponse.builder().build());
    }

    ImmutableList<Event> eventsCopy = ImmutableList.copyOf(storedSession.events());
    return Single.just(ListEventsResponse.builder().events(eventsCopy).build());
  }

  @CanIgnoreReturnValue
  @Override
  public Single<Event> appendEvent(Session session, Event event) {
    Objects.requireNonNull(session, "session cannot be null");
    Objects.requireNonNull(event, "event cannot be null");
    Objects.requireNonNull(session.appName(), "session.appName cannot be null");
    Objects.requireNonNull(session.userId(), "session.userId cannot be null");
    Objects.requireNonNull(session.id(), "session.id cannot be null");

    String appName = session.appName();
    String userId = session.userId();
    String sessionId = session.id();

    EventActions actions = event.actions();
    if (actions != null) {
      Map<String, Object> stateDelta = actions.stateDelta();
      if (stateDelta != null && !stateDelta.isEmpty()) {
        stateDelta.forEach(
            (key, value) -> {
              if (key.startsWith(State.APP_PREFIX)) {
                String appStateKey = key.substring(State.APP_PREFIX.length());
                appState
                    .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
                    .put(appStateKey, value);
              } else if (key.startsWith(State.USER_PREFIX)) {
                String userStateKey = key.substring(State.USER_PREFIX.length());
                userState
                    .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(userId, k -> new ConcurrentHashMap<>())
                    .put(userStateKey, value);
              } else {
                session.state().put(key, value);
              }
            });
      }
    }

    BaseSessionService.super.appendEvent(session, event);
    session.lastUpdateTime(getInstantFromEvent(event));

    sessions
        .getOrDefault(appName, new ConcurrentHashMap<>())
        .getOrDefault(userId, new ConcurrentHashMap<>())
        .put(sessionId, session);

    mergeWithGlobalState(appName, userId, session);

    try {
      writeSession(appName, userId, sessionId, session);
      writeAppState();
      writeUserState();
    } catch (IOException e) {
      LOGGER.error("Failed to write session {}", sessionId, e);
    }

    return Single.just(event);
  }

  private Instant getInstantFromEvent(Event event) {
    double epochSeconds = getEventTimestampEpochSeconds(event);
    long seconds = (long) epochSeconds;
    long nanos = (long) ((epochSeconds % 1.0) * 1_000_000_000L);
    return Instant.ofEpochSecond(seconds, nanos);
  }

  private Session copySession(Session original) {
    return Session.builder(original.id())
        .appName(original.appName())
        .userId(original.userId())
        .state(new ConcurrentHashMap<>(original.state()))
        .events(new ArrayList<>(original.events()))
        .lastUpdateTime(original.lastUpdateTime())
        .build();
  }

  private Session copySessionMetadata(Session original) {
    return Session.builder(original.id())
        .appName(original.appName())
        .userId(original.userId())
        .lastUpdateTime(original.lastUpdateTime())
        .build();
  }

  @CanIgnoreReturnValue
  private Session mergeWithGlobalState(String appName, String userId, Session session) {
    Map<String, Object> sessionState = session.state();

    appState
        .getOrDefault(appName, new ConcurrentHashMap<>())
        .forEach((key, value) -> sessionState.put(State.APP_PREFIX + key, value));

    userState
        .getOrDefault(appName, new ConcurrentHashMap<>())
        .getOrDefault(userId, new ConcurrentHashMap<>())
        .forEach((key, value) -> sessionState.put(State.USER_PREFIX + key, value));

    return session;
  }

  private Session readSession(Path path) throws IOException {
    return Session.fromJson(Files.readString(path));
  }

  private void readAllSessions() {
    var sessionsCount = new AtomicInteger(0);
    try (var appPaths = Files.list(root)) {
      appPaths
          .filter(p -> p.toFile().isDirectory())
          .forEach(
              appPath -> {
                var appName = appPath.getFileName().toString();
                try (var userPaths = Files.list(appPath)) {
                  userPaths
                      .filter(p -> p.toFile().isDirectory())
                      .forEach(
                          userPath -> {
                            var userId = userPath.getFileName().toString();
                            try (var sessionPaths = Files.list(userPath)) {
                              sessionPaths
                                  .filter(p -> p.toFile().isFile())
                                  .forEach(
                                      sessionPath -> {
                                        var sessionId = getSessionId(sessionPath);
                                        try {
                                          var session = readSession(sessionPath);
                                          sessions
                                              .computeIfAbsent(
                                                  appName, k -> new ConcurrentHashMap<>())
                                              .computeIfAbsent(
                                                  userId, k -> new ConcurrentHashMap<>())
                                              .put(sessionId, session);
                                          sessionsCount.incrementAndGet();
                                        } catch (IOException e) {
                                          LOGGER.error("Ignore invalid session {}", sessionId);
                                        }
                                      });
                            } catch (IOException e) {
                              LOGGER.error("Ignore sessions of user id {}", userId, e);
                            }
                          });
                } catch (IOException e) {
                  LOGGER.error("Ignore sessions of app name {}", appName, e);
                }
              });
    } catch (IOException e) {
      LOGGER.error("Failed to read all sessions", e);
    }
    LOGGER.info("Loaded {} sessions", sessionsCount.get());
  }

  private String getSessionId(Path path) {
    String filename = path.getFileName().toString();
    int index = filename.lastIndexOf('.');
    return (index == -1) ? filename : filename.substring(0, index);
  }

  private void readAppState() {
    if (!Files.exists(appStatePath())) {
      return;
    }
    try {
      var state =
          objectMapper.readValue(
              appStatePath().toFile(),
              new TypeReference<ConcurrentMap<String, ConcurrentMap<String, Object>>>() {});
      appState.putAll(state);
    } catch (IOException e) {
      LOGGER.error("Failed to read app state", e);
    }
  }

  private void readUserState() {
    if (!Files.exists(userStatePath())) {
      return;
    }
    try {
      var state =
          objectMapper.readValue(
              userStatePath().toFile(),
              new TypeReference<
                  ConcurrentMap<
                      String, ConcurrentMap<String, ConcurrentMap<String, Object>>>>() {});
      userState.putAll(state);
    } catch (IOException e) {
      LOGGER.error("Failed to read app state", e);
    }
  }

  private void writeAppState() throws IOException {
    objectMapper.writeValue(appStatePath().toFile(), appState);
  }

  private void writeUserState() throws IOException {
    objectMapper.writeValue(userStatePath().toFile(), userState);
  }

  private void writeSession(String appName, String userId, String sessionId, Session session)
      throws IOException {
    var userDir = filePath(appName, userId);
    Files.createDirectories(userDir);
    var sessionPath = filePath(appName, userId, sessionId);
    Files.writeString(
        sessionPath,
        session.toJson(),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  private Path appStatePath() {
    return root.resolve("__app__.json");
  }

  private Path userStatePath() {
    return root.resolve("__user__.json");
  }

  private Path filePath(String appName, String userId) {
    return root.resolve(appName).resolve(userId);
  }

  private Path filePath(String appName, String userId, String sessionId) {
    return filePath(appName, userId).resolve(sessionId + ".json");
  }
}
