package dev.jinhyeon.notificationlab.campaign;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;

/** Avoid silently truncating fractional counts or coercing strings into the stored definition. */
public class CampaignTargetCountDeserializer extends JsonDeserializer<Integer> {
    @Override
    public Integer deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (!parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
            return context.reportInputMismatch(Integer.class, "targetCount must be an integer");
        }
        return parser.getIntValue();
    }
}
