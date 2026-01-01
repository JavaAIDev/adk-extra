package com.javaaidev.adk.artifact.file;


import static org.assertj.core.api.Assertions.assertThat;

import com.google.genai.types.Part;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileBasedArtifactServiceTest {

  private static final String APP_NAME = "test-app";
  private static final String USER_ID = "test-user";
  private static final String SESSION_ID = "test-session";
  private static final String FILENAME = "test-file.txt";
  private static final String USER_FILENAME = "user:config.json";

  private FileBasedArtifactService service;

  @BeforeEach
  void setup() throws IOException {
    service = new FileBasedArtifactService(Files.createTempDirectory("artifacts_"));
  }

  @Test
  void saveArtifact() {
    int version =
        service
            .saveArtifact(
                APP_NAME,
                USER_ID,
                SESSION_ID,
                FILENAME,
                Part.fromBytes("Hello".getBytes(StandardCharsets.UTF_8), "text/plain"))
            .blockingGet();
    assertThat(version).isEqualTo(0);
    var part =
        service
            .loadArtifact(APP_NAME, USER_ID, SESSION_ID, FILENAME, Optional.empty())
            .blockingGet();
    assertThat(part.inlineData()).isPresent();
    var filenames =
        service.listArtifactKeys(APP_NAME, USER_ID, SESSION_ID).blockingGet().filenames();
    assertThat(filenames).hasSize(1);
    service
        .saveArtifact(
            APP_NAME,
            USER_ID,
            SESSION_ID,
            FILENAME,
            Part.fromBytes("World".getBytes(StandardCharsets.UTF_8), "text/plain"))
        .blockingGet();
    var versions = service.listVersions(APP_NAME, USER_ID, SESSION_ID, FILENAME).blockingGet();
    assertThat(versions).hasSize(2);
  }

  @Test
  void saveUserArtifact() {
    int version =
        service
            .saveArtifact(
                APP_NAME,
                USER_ID,
                SESSION_ID,
                USER_FILENAME,
                Part.fromBytes("{}".getBytes(StandardCharsets.UTF_8), "application/json"))
            .blockingGet();
    assertThat(version).isEqualTo(0);
    var part =
        service
            .loadArtifact(APP_NAME, USER_ID, SESSION_ID, USER_FILENAME, Optional.empty())
            .blockingGet();
    assertThat(part.inlineData()).isPresent();
    service
        .saveArtifact(
            APP_NAME,
            USER_ID,
            SESSION_ID,
            FILENAME,
            Part.fromBytes("Hello".getBytes(StandardCharsets.UTF_8), "text/plain"))
        .blockingGet();
    var filenames =
        service.listArtifactKeys(APP_NAME, USER_ID, SESSION_ID).blockingGet().filenames();
    assertThat(filenames).hasSize(2);
  }
}
