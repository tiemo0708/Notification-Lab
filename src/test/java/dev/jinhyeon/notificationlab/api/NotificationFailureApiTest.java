package dev.jinhyeon.notificationlab.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = {
        "app.mock.delay-ms=0", "app.mock.outcome=TRANSIENT_FAILURE", "app.store.max-records=1"
})
@AutoConfigureMockMvc
class NotificationFailureApiTest {
    @Autowired MockMvc mvc;

    @Test
    void failureIsObservableAndFullStoreRejectsSubsequentRequests() throws Exception {
        String payload = "{\"recipientId\":\"user-1\",\"title\":\"t\",\"body\":\"b\"}";
        mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON).content(payload))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("RETRYABLE_FAILURE"))
                .andExpect(jsonPath("$.errorCode").value("MOCK_UNAVAILABLE"));
        mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON).content(payload))
                .andExpect(status().isServiceUnavailable());
    }
}
