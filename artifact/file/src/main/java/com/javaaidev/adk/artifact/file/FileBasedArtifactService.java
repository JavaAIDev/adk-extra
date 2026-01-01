package com.javaaidev.adk.artifact.file;

import com.google.adk.artifacts.BaseArtifactService;
import com.google.adk.artifacts.ListArtifactsResponse;
import com.google.common.collect.ImmutableList;
import com.google.genai.types.Blob;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileBasedArtifactService implements BaseArtifactService {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileBasedArtifactService.class);
  private static final String USER_SCOPE_SESSION_ID = "__user__";
  private final Path root;

  public FileBasedArtifactService(Path root) {
    Objects.requireNonNull(root, "root path cannot be null");
    if (!Files.exists(root)) {
      try {
        Files.createDirectories(root);
      } catch (IOException e) {
        LOGGER.error("Failed to create artifacts directory", e);
      }
    }
    this.root = root.normalize().toAbsolutePath();
    LOGGER.info("Artifacts saved to {}", this.root);
  }

  @Override
  public Single<Integer> saveArtifact(
      String appName, String userId, String sessionId, String filename, Part artifact) {
    try {
      var pathAndVersion = filePath(appName, userId, sessionId, filename, false, Optional.empty());
      var data =
          artifact
              .inlineData()
              .flatMap(Blob::data)
              .orElseThrow(() -> new IllegalArgumentException("Artifact data must be non-empty."));
      Files.write(pathAndVersion.path(), data);
      return Single.just(pathAndVersion.version());
    } catch (Exception e) {
      return Single.error(e);
    }
  }

  @Override
  public Maybe<Part> loadArtifact(
      String appName, String userId, String sessionId, String filename, Optional<Integer> version) {
    try {
      var pathAndVersion = filePath(appName, userId, sessionId, filename, true, version);
      var data = Files.readAllBytes(pathAndVersion.path());
      return Maybe.just(Part.fromBytes(data, "application/octet-stream"));
    } catch (IOException e) {
      return Maybe.error(e);
    }
  }

  @Override
  public Single<ListArtifactsResponse> listArtifactKeys(
      String appName, String userId, String sessionId) {
    var filenames = new ArrayList<String>();
    filenames.addAll(getFilenames(filePath(appName, userId, sessionId)));
    filenames.addAll(getFilenames(filePath(appName, userId, USER_SCOPE_SESSION_ID)));
    return Single.just(ListArtifactsResponse.builder().filenames(filenames).build());
  }

  private List<String> getFilenames(Path path) {
    try (var files = Files.list(path)) {
      return files.map(p -> p.getFileName().toString()).toList();
    } catch (Exception e) {
      return List.of();
    }
  }

  @Override
  public Completable deleteArtifact(
      String appName, String userId, String sessionId, String filename) {
    var path = filePath(appName, userId, sessionId, filename);
    try {
      Files.delete(path);
      return Completable.complete();
    } catch (IOException e) {
      return Completable.error(e);
    }
  }

  @Override
  public Single<ImmutableList<Integer>> listVersions(
      String appName, String userId, String sessionId, String filename) {
    var path = filePath(appName, userId, sessionId, filename);
    try (var files = Files.list(path)) {
      var versions =
          files.map(p -> p.getFileName().toString()).map(Integer::parseInt).sorted().toList();
      return Single.just(ImmutableList.copyOf(versions));
    } catch (Exception e) {
      return Single.error(e);
    }
  }

  private PathAndVersion filePath(
      String appName,
      String userId,
      String sessionId,
      String filename,
      boolean read,
      Optional<Integer> readVersion)
      throws IOException {
    var path = filePath(appName, userId, sessionId, filename);
    int version;
    if (!Files.exists(path)) {
      version = 0;
      Files.createDirectories(path);
    } else {
      try (var files = Files.list(path)) {
        var maxVersion =
            files.map(p -> p.getFileName().toString()).map(Integer::parseInt).max(Integer::compare);
        version =
            read ? readVersion.orElse(maxVersion.orElse(0)) : maxVersion.map(v -> v + 1).orElse(0);
      }
    }
    return new PathAndVersion(path.resolve(String.valueOf(version)), version);
  }

  private Path filePath(String appName, String userId, String sessionId) {
    return root.resolve(appName).resolve(userId).resolve(sessionId);
  }

  private Path filePath(String appName, String userId, String sessionId, String filename) {
    var sid = fileHasUserNamespace(filename) ? USER_SCOPE_SESSION_ID : sessionId;
    return filePath(appName, userId, sid).resolve(filename);
  }

  private boolean fileHasUserNamespace(String filename) {
    return filename != null && filename.startsWith("user:");
  }

  record PathAndVersion(Path path, Integer version) {}
}
