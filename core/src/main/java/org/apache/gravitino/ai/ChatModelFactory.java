package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.apache.gravitino.Config;

public class ChatModelFactory {

  // TODO support more models
  public static ChatModel createChatModel(Config config) {
    return OpenAiChatModel.builder()
        .baseUrl("https://api.deepseek.com/v1") // e.g., "http://localhost:8000/v1"
        .apiKey("") // e.g., "sk-yourkey" or "none"
        .modelName("deepseek-chat") // e.g., "" or custom name
        // Add other configurations like temperature, timeout, etc. as needed
        .logRequests(true)
        .logResponses(true)
        .build();
  }
}
