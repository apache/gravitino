package org.apache.gravitino.ai;

import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.apache.gravitino.Config;

/**
 * TODO Since different users may use different AI models, we need to consider how to support
 * multiple AI models in the absence of auto-configuration capabilities.
 * However, this is a lower priority because the OpenAI API protocol can support most AI models.
 */
public class ChatModelFactory {

  public static ChatModel createChatModel(Config config) {
    //TODO Load the config required to create a ChatModel from the configuration file.
    return OpenAiChatModel.builder()
        .baseUrl("https://api.deepseek.com/v1") // e.g.,
        .apiKey("") // e.g., "sk-yourkey" or "none"
        .modelName("deepseek-chat")
        // Add other configurations like temperature, timeout, etc. as needed
        .logRequests(true)
        .logResponses(true)
        .build();
  }
}
