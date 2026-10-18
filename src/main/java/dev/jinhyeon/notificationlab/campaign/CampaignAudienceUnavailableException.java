package dev.jinhyeon.notificationlab.campaign;

public class CampaignAudienceUnavailableException extends RuntimeException {
    public CampaignAudienceUnavailableException() {
        super("Campaign audience definition is unavailable");
    }
}
