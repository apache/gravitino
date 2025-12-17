package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import org.apache.gravitino.Config;

/**
 * Used to store Langchain4j ChatModel instances.
 * TODO: Similarly, we will also need various RAG-related objects,
 * such as Embedding models and vector stores.
 */
public class ChatModelProvider {

  private volatile ChatModel chatModel;

  private static final ChatModelProvider INSTANCE = new ChatModelProvider();

  private ChatModelProvider() {}

  public static ChatModelProvider getInstance() {
    return INSTANCE;
  }

  public void init(Config config) {
    if (chatModel == null) {
      synchronized (ChatModelProvider.class) {
        if (chatModel == null) {
          ChatModelFactory.createChatModel(config);
        }
      }
    }
  }

  public ChatModel getChatModel() {
    return chatModel;
  }
}
