package dev.jinhyeon.notificationlab.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = "app.mock.delay-ms=0")
@AutoConfigureMockMvc
@AutoConfigureObservability
@ActiveProfiles("memory")
class NotificationApiTest {
    @Autowired MockMvc mvc;

    @Test
    void createAndLookupSynchronousResult() throws Exception {
        var response = mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"recipientId":"user-100","title":"테스트","body":"알림 처리 확인"}
                            """))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", startsWith("/notifications/")))
                .andExpect(jsonPath("$.status").value("PROVIDER_ACCEPTED"))
                .andExpect(jsonPath("$.completedAt").isNotEmpty())
                .andReturn().getResponse();
        mvc.perform(get(response.getHeader("Location")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.recipientId").value("user-100"));
    }

    @Test
    void memoryProfileRejectsIdempotencyHeaderInsteadOfSilentlyIgnoringIt() throws Exception {
        mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "comparison-key")
                        .content("""
                                {"recipientId":"user-100","title":"title","body":"body"}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void redriveIsNotAvailableInMemoryProfile() throws Exception {
        String path = "/dead-letters/00000000-0000-0000-0000-000000000001";
        mvc.perform(get(path)).andExpect(status().isNotFound());
        mvc.perform(post(path + "/redrive").contentType(MediaType.APPLICATION_JSON)
                .content("{\"reason\":\"MANUAL_REVIEW\",\"acknowledgePossibleDuplicate\":true}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void emptyRecipientIsRejected() throws Exception {
        mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"recipientId\":\" \",\"title\":\"t\",\"body\":\"b\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void oversizeBodyIsRejected() throws Exception {
        mvc.perform(post("/notifications").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"recipientId\":\"u\",\"title\":\"t\",\"body\":\"" + "x".repeat(501) + "\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void missingAndMalformedIdsAreDistinguished() throws Exception {
        mvc.perform(get("/notifications/00000000-0000-0000-0000-000000000000"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/notifications/not-a-uuid"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void healthAndPrometheusAreAvailableButEnvironmentIsNotExposed() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
        String body = mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(body).contains("jvm_memory_used_bytes");
        mvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
    }
}
