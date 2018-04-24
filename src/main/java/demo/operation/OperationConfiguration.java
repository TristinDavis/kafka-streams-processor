package demo.operation;

import demo.operation.configuration.KafkaStreamProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaStreamProperties.class)
public class OperationConfiguration {

}
