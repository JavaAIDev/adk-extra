package com.javaaidev.adk.session.file;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.adk.events.Event;
import com.google.adk.events.EventActions;
import com.google.adk.sessions.ListSessionsResponse;
import com.google.adk.sessions.Session;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileBasedSessionServiceTest {

  private FileBasedSessionService sessionService;

  @BeforeEach
  void setup() throws IOException {
    sessionService = new FileBasedSessionService(Files.createTempDirectory("session_"));
  }

  @Test
  public void lifecycle_noSession() {
    assertThat(
            sessionService
                .getSession("app-name", "user-id", "session-id", Optional.empty())
                .blockingGet())
        .isNull();

    assertThat(sessionService.listSessions("app-name", "user-id").blockingGet().sessions())
        .isEmpty();

    assertThat(
            sessionService.listEvents("app-name", "user-id", "session-id").blockingGet().events())
        .isEmpty();
  }

  @Test
  public void lifecycle_createSession() {

    Single<Session> sessionSingle = sessionService.createSession("app-name", "user-id");

    Session session = sessionSingle.blockingGet();

    assertThat(session.id()).isNotNull();
    assertThat(session.appName()).isEqualTo("app-name");
    assertThat(session.userId()).isEqualTo("user-id");
    assertThat(session.state()).isEmpty();
  }

  @Test
  public void lifecycle_getSession() {

    Session session = sessionService.createSession("app-name", "user-id").blockingGet();

    Session retrievedSession =
        sessionService
            .getSession(session.appName(), session.userId(), session.id(), Optional.empty())
            .blockingGet();

    assertThat(retrievedSession).isNotNull();
    assertThat(retrievedSession.id()).isEqualTo(session.id());
  }

  @Test
  public void lifecycle_listSessions() {
    Session session = sessionService.createSession("app-name", "user-id").blockingGet();

    ListSessionsResponse response =
        sessionService.listSessions(session.appName(), session.userId()).blockingGet();

    assertThat(response.sessions()).hasSize(1);
    assertThat(response.sessions().get(0).id()).isEqualTo(session.id());
  }

  @Test
  public void lifecycle_deleteSession() {

    Session session = sessionService.createSession("app-name", "user-id").blockingGet();

    sessionService.deleteSession(session.appName(), session.userId(), session.id()).blockingAwait();

    assertThat(
            sessionService
                .getSession(session.appName(), session.userId(), session.id(), Optional.empty())
                .blockingGet())
        .isNull();
  }

  @Test
  public void appendEvent_updatesSessionState() {
    Session session =
        sessionService
            .createSession("app", "user", new ConcurrentHashMap<>(), "session1")
            .blockingGet();

    ConcurrentMap<String, Object> stateDelta = new ConcurrentHashMap<>();
    stateDelta.put("sessionKey", "sessionValue");
    stateDelta.put("_app_appKey", "appValue");
    stateDelta.put("_user_userKey", "userValue");
    stateDelta.put("app:key1", "value1");
    stateDelta.put("user:key2", "value2");

    Event event =
        Event.builder().actions(EventActions.builder().stateDelta(stateDelta).build()).build();

    sessionService.appendEvent(session, event).blockingGet();

    assertThat(session.state()).containsEntry("sessionKey", "sessionValue");
    assertThat(session.state()).containsEntry("_app_appKey", "appValue");
    assertThat(session.state()).containsEntry("_user_userKey", "userValue");

    Session retrievedSession =
        sessionService
            .getSession(session.appName(), session.userId(), session.id(), Optional.empty())
            .blockingGet();
    assertThat(retrievedSession.state()).containsEntry("sessionKey", "sessionValue");
    assertThat(retrievedSession.state()).containsEntry("_app_appKey", "appValue");
    assertThat(retrievedSession.state()).containsEntry("_user_userKey", "userValue");
    assertThat(retrievedSession.state()).containsEntry("app:key1", "value1");
    assertThat(retrievedSession.state()).containsEntry("user:key2", "value2");
  }
}
