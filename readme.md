### Instrument normalizer

```shell
-javaagent:/Users/limi/projects/extb/instrument-normalizer/lib/opentelemetry-javaagent.jar -Dotel.resource.attributes="service.name=KSA_Receiver" -Dotel.metrics.exporter=none -Dotel.traces.exporter=zipkin -Dotel.exporter.zipkin.endpoint=http://localhost:9411/api/v2/spans
```