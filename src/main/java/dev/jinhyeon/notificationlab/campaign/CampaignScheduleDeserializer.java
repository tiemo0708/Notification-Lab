package dev.jinhyeon.notificationlab.campaign;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class CampaignScheduleDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        if (parser.hasToken(JsonToken.VALUE_STRING)) {
            try {
                var utc = OffsetDateTime.parse(parser.getText()).withOffsetSameInstant(ZoneOffset.UTC);
                if (utc.getYear() >= 1000 && utc.getYear() <= 9999 && utc.getNano() % 1000 == 0) {
                    return utc.toInstant();
                }
            } catch (DateTimeException ignored) {
                // Return the same input error for all invalid timestamp forms.
            }
        }
        return context.reportInputMismatch(Instant.class, "scheduledAt requires an ISO timestamp with an offset and microsecond precision");
    }
}
