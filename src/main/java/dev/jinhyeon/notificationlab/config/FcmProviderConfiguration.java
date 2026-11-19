package dev.jinhyeon.notificationlab.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.provider.FcmPushProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.provider.mode", havingValue = "FCM")
@EnableConfigurationProperties(FcmLabProperties.class)
public class FcmProviderConfiguration {
    @Bean(destroyMethod = "close")
    FcmPushProvider fcmPushProvider(FcmLabProperties settings, ObjectMapper mapper, Environment environment) {
        if (environment.acceptsProfiles(Profiles.of("memory", "sync"))
                || !"127.0.0.1".equals(environment.getProperty("server.address"))
                || environment.getProperty("app.delivery.max-attempts", Integer.class, 3) != 1) {
            throw new IllegalArgumentException("FCM lab requires an async loopback app with one delivery attempt");
        }
        return new FcmPushProvider(settings, mapper);
    }
}
