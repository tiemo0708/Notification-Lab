package dev.jinhyeon.notificationlab.campaign;

public class CampaignNotFoundException extends RuntimeException {
    public CampaignNotFoundException() { super("Campaign not found"); }
}
