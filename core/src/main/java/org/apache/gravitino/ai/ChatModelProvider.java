package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import org.apache.gravitino.Config;

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
