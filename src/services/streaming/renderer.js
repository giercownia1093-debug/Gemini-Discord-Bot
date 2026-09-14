import { logServiceError } from '../../utils/errorHandler.js';

function logRendererError(operation, error, metadata = {}) {
  logServiceError('StreamingService', error, { operation, ...metadata });
}

/**
 * Edits the bot message to display the accumulated response text as clean plain text.
 */
export async function buildResponseEmbed(botMessage, text, originalMessage, groundingMetadata = null, urlContextMetadata = null) {
  try {
    const trimmedText = text.trim() || '.';
    
    await botMessage.edit({
      content: trimmedText,
      embeds: [],
      components: botMessage.components,
    });
  } catch (error) {
    logRendererError('buildResponseText', error, { messageId: botMessage.id });
  }
}
