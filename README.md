# Metrics Datadog Reporter
Simple Metrics reporter that sends The Goods to Datadog. Real person
documentation pending

## Metrics Version
Version updated to work with Codahale Metrics 3.0.1

## Usage

~~~java
import com.codahale.metrics.reporting.DatadogReporter

...
final MetricRegistry registry = new MetricRegistry();
final Transport transport = new HttpTransport("app.datadoghq.com", "xxxxxxxxxx");
final String host = "myhost";

DatadogReporter reporter = DatadogReporter.forRegistry(registry)
								.convertRatesTo(TimeUnit.SECONDS)
								.convertDurationsTo(TimeUnit.MILLISECONDS)
								.build(transport, host);
reporter.start(15, TimeUnit.SECONDS);
~~~

## Maven

This version has not been released on a public Maver repository.
You will have to take care of building it yourself; sorry.

Changed `group-id` from `com.vistarmedia` to `com.clipperz` to avoid clashing in version numbers.