package dev.jinhyeon.notificationlab.config;

import java.util.HashSet;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component("trafficRouting")
@Profile("!memory & !sync")
public class TrafficRouting {
    private final AsyncProperties async;
    private final TrafficProperties traffic;

    public TrafficRouting(AsyncProperties async, TrafficProperties traffic,
                          @Value("${app.rate-limit.client-enabled:false}") boolean quota,
                          @Value("${app.rate-limit.pause-enabled:true}") boolean pause) {
        if (traffic.splitEnabled() && (!quota || !pause)) {
            throw new IllegalArgumentException("Traffic split requires shared Redis quota and consumer pause");
        }
        if (traffic.splitEnabled()) {
            var names = new HashSet<String>();
            for (String base : List.of(async.topic(), async.retryTopic(), async.deadLetterTopic())) {
                if (!names.add(base) || !names.add(base + ".campaign")) {
                    throw new IllegalArgumentException("Traffic topics must be distinct across both roles");
                }
            }
        }
        this.async = async;
        this.traffic = traffic;
    }

    public String topic() { return traffic.topic(async.topic()); }
    public String retryTopic() { return traffic.topic(async.retryTopic()); }
    public String deadLetterTopic() { return traffic.topic(async.deadLetterTopic()); }
    public String group() { return traffic.topic(async.group()); }
    public String destination(String type) { return traffic.topic(async.destination(type)); }
}
