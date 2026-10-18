package dev.jinhyeon.notificationlab.campaign;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Only a bounded synthetic audience is supported; no recipient list is accepted. */
public record CampaignRequest(@NotBlank @Size(max = 100) String name,
                              @NotBlank @Size(max = 100) String title,
                              @NotBlank @Size(max = 500) String body,
                              @JsonDeserialize(using = CampaignTargetCountDeserializer.class)
                              @NotNull @Min(1) @Max(1_000_000) Integer targetCount) { }
