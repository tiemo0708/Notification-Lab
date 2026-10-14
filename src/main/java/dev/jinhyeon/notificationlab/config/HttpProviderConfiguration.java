package dev.jinhyeon.notificationlab.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.jinhyeon.notificationlab.provider.HttpPushProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ProviderProperties.class)
public class HttpProviderConfiguration {
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "app.provider.mode", havingValue = "HTTP")
    HttpPushProvider httpPushProvider(ProviderProperties settings, ObjectMapper mapper) {
        return new HttpPushProvider(settings.http(), mapper);
    }
}
