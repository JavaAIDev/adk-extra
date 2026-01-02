package com.javaaidev.adk.memory.vectorstore;

import com.google.adk.memory.BaseMemoryService;
import com.google.adk.memory.MemoryEntry;
import com.google.adk.memory.SearchMemoryResponse;
import com.google.adk.sessions.Session;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.ai.vectorstore.filter.FilterExpressionBuilder;

/** {@linkplain BaseMemoryService} implementation using Spring AI {@linkplain VectorStore} */
public class VectorStoreMemoryService implements BaseMemoryService {

  private final VectorStore vectorStore;
  private final SearchRequest searchRequest;

  public static final String METADATA_APP_NAME = "adk.appName";
  public static final String METADATA_USER_ID = "adk.userId";
  public static final String METADATA_SESSION_ID = "adk.sessionId";

  /**
   * @param vectorStore Spring AI {@linkplain VectorStore}
   */
  public VectorStoreMemoryService(VectorStore vectorStore) {
    this(vectorStore, SearchRequest.builder().topK(3).build());
  }

  /**
   * @param vectorStore Spring AI {@linkplain VectorStore}
   * @param searchRequest Base {@linkplain SearchRequest} to search for documents
   */
  public VectorStoreMemoryService(VectorStore vectorStore, SearchRequest searchRequest) {
    Objects.requireNonNull(vectorStore, "vectorStore cannot be null");
    Objects.requireNonNull(searchRequest, "searchRequest cannot be null");
    this.vectorStore = vectorStore;
    this.searchRequest = searchRequest;
  }

  @Override
  public Completable addSessionToMemory(Session session) {
    var text =
        session.events().stream()
            .flatMap(event -> event.content().flatMap(Content::parts).stream())
            .flatMap(Collection::stream)
            .flatMap(part -> part.text().stream())
            .collect(Collectors.joining("\n"));
    var doc =
        Document.builder()
            .id(session.id())
            .text(text)
            .metadata(METADATA_APP_NAME, session.appName())
            .metadata(METADATA_USER_ID, session.userId())
            .metadata(METADATA_SESSION_ID, session.id())
            .build();
    try {
      vectorStore.add(List.of(doc));
      return Completable.complete();
    } catch (Exception e) {
      return Completable.error(e);
    }
  }

  @Override
  public Single<SearchMemoryResponse> searchMemory(String appName, String userId, String query) {
    var builder = new FilterExpressionBuilder();
    var expression =
        builder
            .and(builder.eq(METADATA_APP_NAME, appName), builder.eq(METADATA_USER_ID, userId))
            .build();
    var docs =
        vectorStore.similaritySearch(
            SearchRequest.from(searchRequest).query(query).filterExpression(expression).build());
    if (docs == null || docs.isEmpty()) {
      return Single.just(SearchMemoryResponse.builder().build());
    }
    var memoryEntries =
        docs.stream()
            .map(
                doc ->
                    MemoryEntry.builder()
                        .content(Content.fromParts(Part.fromText(doc.getText())))
                        .build())
            .toList();
    return Single.just(SearchMemoryResponse.builder().setMemories(memoryEntries).build());
  }
}
