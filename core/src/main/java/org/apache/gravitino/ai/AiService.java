package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.ai.prompt.Prompts;
import org.apache.gravitino.rel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create various asynchronous AI tasks, such as evaluating table schemas, auto-tagging, etc.
 *
 * TODO We need to consider how to integrate RAG capabilities to support richer functionality.
 */
public class AiService {

  private static final Logger log = LoggerFactory.getLogger(AiService.class);

  private static final Executor executor = Executors.newFixedThreadPool(20);

  private static final AiService instance = new AiService();

  public static AiService getInstance() {
    return instance;
  }

  private AiService() {}

    /**
     * After a table is created, we can use AI to evaluate its schema, store the evaluation results
     * for users to review, and also notify users via alerting channels (such as email, DingTalk,
     * Feishu, etc.). In the future, we can generate assessment metrics such as table quality
     * and schema quality.
     * @param table
     * @param nameIdentifier
     */
  public void asyncAiEvaluation(Table table, NameIdentifier nameIdentifier) {
      // TODO Create a queue in the database to persist all AI asynchronous tasks, and then process them
      //  using asynchronous threads to ensure that every AI async task gets executed.
    CompletableFuture.runAsync(
        () -> {
          ChatModel chatModel = ChatModelProvider.getInstance().getChatModel();
          String evaluationResult =
                  // TODO Use langchain4j AI Service
              chatModel.chat(
                      // TODO We need to enable users to customize system prompts and
                      //  support injecting different  rules for different users without restarting the service.
                  Prompts.CREATE_TABLE_EVALUATION.formatted(
                      nameIdentifier,
                      Arrays.toString(table.columns()),
                      "paimon", // TODO Get table type from catalog name
                      Arrays.toString(table.index()),
                      "", // TODO Get partition info
                      table.properties()));
          // TODO Persist result to database and support querying via REST API.
          log.info("Evaluation result: {}", evaluationResult);
        },
        executor);
  }

  // TODO We need to expose the Tag API as a Langchain4j tool, enabling the AI to query relevant tags
  //  and then either automatically assign tags or provide tag suggestions based on the retrieved
  //  information.
  public void asyncAiAutoTag(Table table, NameIdentifier nameIdentifier) {

  }
}
