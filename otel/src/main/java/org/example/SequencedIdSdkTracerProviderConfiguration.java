package org.example;

import com.google.auto.service.AutoService;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;
import io.opentelemetry.sdk.autoconfigure.spi.ConfigProperties;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.Map;
import java.util.function.BiFunction;

@AutoService(AutoConfigurationCustomizerProvider.class)
public class SequencedIdSdkTracerProviderConfiguration
    implements AutoConfigurationCustomizerProvider {
  @Override
  public void customize(AutoConfigurationCustomizer autoConfiguration) {
    autoConfiguration.addTracerProviderCustomizer(
        new BiFunction<SdkTracerProviderBuilder, ConfigProperties, SdkTracerProviderBuilder>() {
          @Override
          public SdkTracerProviderBuilder apply(
              SdkTracerProviderBuilder sdkTracerProviderBuilder,
              ConfigProperties configProperties) {

            String serviceName = "unknown_service:java";
            String instanceName = "unknown_service:java:instance";

            Map<String, String> attributes = configProperties.getMap("otel.resource.attributes");
            String serviceNameValue = attributes.get(ResourceAttributes.SERVICE_NAME.getKey());
            if (serviceNameValue != null && !serviceNameValue.isEmpty()) {
              serviceName = serviceNameValue;
            }
            String serviceInstanceIdValue =
                attributes.get(ResourceAttributes.SERVICE_INSTANCE_ID.getKey());
            if (serviceInstanceIdValue != null && !serviceInstanceIdValue.isEmpty()) {
              instanceName = serviceInstanceIdValue;
            }

            sdkTracerProviderBuilder.setIdGenerator(
                new SequencedIdGenerator(serviceName, instanceName));
            return sdkTracerProviderBuilder;
          }
        });
  }
}
