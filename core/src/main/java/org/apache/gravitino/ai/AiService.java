package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.ai.prompt.Prompts;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AiService {

  private static final Logger log = LoggerFactory.getLogger(AiService.class);

  private static final Executor executor = Executors.newFixedThreadPool(20);

  private static final AiService instance = new AiService();

  public static AiService getInstance() {
    return instance;
  }

  private AiService() {}

  public void asyncAiEvaluation(Table table, NameIdentifier nameIdentifier) {
    CompletableFuture.runAsync(
        () -> {
          ChatModel chatModel = ChatModelProvider.getInstance().getChatModel();
          String evaluationResult =
              chatModel.chat(
                  // TODO support load prompts from config file.
                  Prompts.CREATE_TABLE_EVALUATION.formatted(
                      nameIdentifier,
                      Arrays.toString(table.columns()),
                      "paimon",
                      Arrays.toString(table.index()),
                      "",
                      table.properties()));
          // TODO Persist to database and support querying via REST API.
          log.info("Evaluation result: {}", evaluationResult);
        },
        executor);
  }
}
