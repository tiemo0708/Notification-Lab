package dev.jinhyeon.notificationlab.campaign;

public class InvalidCampaignScheduleException extends RuntimeException {
    public InvalidCampaignScheduleException() { super("scheduledAt must be in the future"); }
}
