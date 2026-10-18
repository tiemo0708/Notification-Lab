package dev.jinhyeon.notificationlab.campaign;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SyntheticCampaignAudienceTest {
    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    @Test
    void versionOneHasStableNumericKeysAndExplicitTerminalPages() {
        var campaign = campaign("SYNTHETIC_V1", 12);
        var first = SyntheticCampaignAudience.read(campaign, 0, 10);
        assertThat(first.campaignId()).isEqualTo(ID);
        assertThat(first.audience()).isEqualTo("SYNTHETIC_V1");
        assertThat(first.targetCount()).isEqualTo(12);
        assertThat(first.items()).hasSize(10);
        assertThat(first.items().getFirst()).isEqualTo(new CampaignAudiencePage.Target(1, "synthetic-v1-1"));
        assertThat(first.items().getLast()).isEqualTo(new CampaignAudiencePage.Target(10, "synthetic-v1-10"));
        assertThat(first.nextAfter()).isEqualTo(10L);
        var last = SyntheticCampaignAudience.read(campaign, first.nextAfter(), 10);
        assertThat(last.items()).containsExactly(new CampaignAudiencePage.Target(11, "synthetic-v1-11"),
                new CampaignAudiencePage.Target(12, "synthetic-v1-12"));
        assertThat(last.nextAfter()).isNull();
        var terminal = SyntheticCampaignAudience.read(campaign, 12, 10);
        assertThat(terminal.items()).isEmpty();
        assertThat(terminal.nextAfter()).isNull();
    }

    @ParameterizedTest
    @CsvSource({
            "100000,05c4bfd7158adda6f40c30af79c4d499798d0123af4c2a2cd4529c6c01e8c105",
            "1000000,f573d9fac181278ccd18031cd727fc925165fa4e2d4df0d1f3e5d83ea57b9764"
    })
    void fullTraversalMatchesFrozenVersionOneFixtureWithBoundedChangingPages(int count, String expectedHash) throws Exception {
        var campaign = campaign("SYNTHETIC_V1", count);
        var digest = MessageDigest.getInstance("SHA-256");
        long after = 0;
        long visited = 0;
        int pages = 0;
        while (true) {
            int limit = pages % 2 == 0 ? 997 : 1000;
            var page = SyntheticCampaignAudience.read(campaign, after, limit);
            assertThat(page.items()).isNotEmpty().hasSizeLessThanOrEqualTo(limit);
            for (var target : page.items()) {
                assertThat(target.ordinal()).isEqualTo(++visited);
                digest.update((target.ordinal() + ":" + target.recipientId() + "\n").getBytes(StandardCharsets.UTF_8));
            }
            pages++;
            if (page.nextAfter() == null) break;
            assertThat(page.nextAfter()).isEqualTo(visited);
            after = page.nextAfter();
        }
        assertThat(visited).isEqualTo(count);
        assertThat(HexFormat.of().formatHex(digest.digest())).isEqualTo(expectedHash);
        System.out.printf("CAMPAIGN_AUDIENCE targets=%d pages=%d maxPage=1000 sha256=%s%n", count, pages, expectedHash);
    }

    @Test
    void savedKeyResumesIdenticallyAndPageSizeCanChangeWithoutChangingMembership() {
        var campaign = campaign("SYNTHETIC_V1", 1_000_000);
        var deep = SyntheticCampaignAudience.read(campaign, 999997, 2);
        assertThat(deep.items()).containsExactly(new CampaignAudiencePage.Target(999998, "synthetic-v1-999998"),
                new CampaignAudiencePage.Target(999999, "synthetic-v1-999999"));
        assertThat(deep.nextAfter()).isEqualTo(999999L);
        assertThat(SyntheticCampaignAudience.read(campaign("SYNTHETIC_V1", 1_000_000), 999997, 2)).isEqualTo(deep);
        var finalPage = SyntheticCampaignAudience.read(campaign, deep.nextAfter(), 1000);
        assertThat(finalPage.items()).containsExactly(new CampaignAudiencePage.Target(1000000, "synthetic-v1-1000000"));
        assertThat(finalPage.nextAfter()).isNull();
        assertThat(SyntheticCampaignAudience.read(campaign("SYNTHETIC_V1", 1), 0, 1000).items())
                .containsExactly(new CampaignAudiencePage.Target(1, "synthetic-v1-1"));
    }

    @Test
    void invalidCursorAndUnboundedSizesAreRejectedBeforeAllocation() {
        var campaign = campaign("SYNTHETIC_V1", 12);
        for (long after : new long[]{-1, 13, Long.MIN_VALUE, Long.MAX_VALUE}) {
            assertThatThrownBy(() -> SyntheticCampaignAudience.read(campaign, after, 100))
                    .isInstanceOf(InvalidCampaignPageException.class);
        }
        for (int limit : new int[]{-1, 0, 1001, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
            assertThatThrownBy(() -> SyntheticCampaignAudience.read(campaign, 0, limit))
                    .isInstanceOf(InvalidCampaignPageException.class);
        }
    }

    @Test
    void unknownVersionOrCorruptDefinitionDoesNotSilentlyChooseDifferentMembers() {
        for (var campaign : List.of(campaign("SYNTHETIC_V2", 12), campaign("SYNTHETIC_V1", 0),
                campaign("SYNTHETIC_V1", 1000001))) {
            assertThatThrownBy(() -> SyntheticCampaignAudience.read(campaign, 0, 100))
                    .isInstanceOf(CampaignAudienceUnavailableException.class);
        }
    }

    @Test
    void callersCannotMutateAReturnedPage() {
        var page = SyntheticCampaignAudience.read(campaign("SYNTHETIC_V1", 12), 0, 2);
        assertThatThrownBy(() -> page.items().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThat(SyntheticCampaignAudience.read(campaign("SYNTHETIC_V1", 12), 0, 2)).isEqualTo(page);
    }

    private Campaign campaign(String version, int count) {
        return new Campaign(ID, "가상 집합", "제목", "본문", version, count,
                Campaign.Status.DRAFT, Instant.parse("2026-10-18T00:00:00Z"), null);
    }
}
