package dev.jinhyeon.notificationlab.campaign;

public class InvalidCampaignPageException extends RuntimeException {
    public InvalidCampaignPageException() {
        super("Invalid campaign audience cursor or page size");
    }
}
