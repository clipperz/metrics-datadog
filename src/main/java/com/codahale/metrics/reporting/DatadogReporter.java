package com.codahale.metrics.reporting;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Sampling;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.codahale.metrics.reporting.model.DatadogCounter;
import com.codahale.metrics.reporting.model.DatadogGauge;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatadogReporter extends ScheduledReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatadogReporter.class);

    private final Clock clock;
    private final String prefix;
    private final HttpTransport transport;
	private final String host;

	private static final JsonFactory jsonFactory = new JsonFactory();
	private static final ObjectMapper mapper = new ObjectMapper(jsonFactory);


	//==========================================================================
	
	public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private String prefix;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.prefix = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder prefixedWith(String prefix) {
            this.prefix = prefix;
            return this;
        }

        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public DatadogReporter build(HttpTransport transport, String host) {
            return new DatadogReporter(registry, transport, host, clock, prefix, rateUnit, durationUnit, filter);
        }
    }
	
	//==========================================================================
	
    private DatadogReporter(
		MetricRegistry registry,
		HttpTransport transport,
		String host,
		Clock clock,
		String prefix,
		TimeUnit rateUnit,
		TimeUnit durationUnit,
		MetricFilter filter
	) {
		super(registry, "datadog-reporter", filter, rateUnit, durationUnit);
		this.transport = transport;
		this.host = host;
		this.clock = clock;
		this.prefix = prefix;
    }
	
	protected String sanitizeName(String name, String... path) {
		final StringBuilder sb = new StringBuilder(name);

		for (String part : path) {
			sb.append('.').append(part);
		}

		return sb.toString();
	}
	protected void writeGauge(JsonGenerator jsonOut, String name, Number value, Long timestamp, String... path) throws IOException {
	    DatadogGauge gauge = new DatadogGauge(this.sanitizeName(name, path), value, timestamp, this.host);
		mapper.writeValue(jsonOut, gauge);
	}
	
	protected void writeCounter(JsonGenerator jsonOut, String name, long value, Long timestamp, String... path) throws IOException {
		DatadogCounter counter = new DatadogCounter(this.sanitizeName(name, path), value, timestamp, this.host);
		mapper.writeValue(jsonOut, counter);
	}

	protected void writeSampling(JsonGenerator jsonOut, String name, Sampling sampling, Long timestamp) throws IOException {
		final Snapshot snapshot = sampling.getSnapshot();
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.getMin()),				timestamp, "min");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.getMax()),				timestamp, "max");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.getMean()),			timestamp, "mean");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.getStdDev()),			timestamp, "stddev");

		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.getMedian()),			timestamp, "median");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.get75thPercentile()),	timestamp, "75percentile");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.get95thPercentile()),	timestamp, "95percentile");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.get98thPercentile()),	timestamp, "98percentile");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.get99thPercentile()),	timestamp, "99percentile");
		this.writeGauge(jsonOut, name, this.convertDuration(snapshot.get999thPercentile()),	timestamp, "999percentile");
	}

	protected void writeMetered(JsonGenerator jsonOut, String name, Metered meter, Long timestamp) throws IOException {
		this.writeCounter(jsonOut, name, meter.getCount(), timestamp);
		this.writeGauge(jsonOut, name, this.convertRate(meter.getMeanRate()),			timestamp, "mean");
		this.writeGauge(jsonOut, name, this.convertRate(meter.getOneMinuteRate()),		timestamp, "1MinuteRate");
		this.writeGauge(jsonOut, name, this.convertRate(meter.getFiveMinuteRate()),		timestamp, "5MinuteRate");
		this.writeGauge(jsonOut, name, this.convertRate(meter.getFifteenMinuteRate()),	timestamp, "15MinuteRate");
	}
	
	@Override public void report(
		SortedMap<String, Gauge> gauges,
		SortedMap<String, Counter> counters,
		SortedMap<String, Histogram> histograms,
		SortedMap<String, Meter> meters,
		SortedMap<String, Timer> timers
	) {
		final long timestamp = clock.getTime() / 1000;
		JsonGenerator		jsonOut;
		Transport.Request	request;

		try {
			request = transport.prepare();
			jsonOut = jsonFactory.createJsonGenerator(request.getBodyWriter());
			jsonOut.writeStartObject();
			jsonOut.writeFieldName("series");
			jsonOut.writeStartArray();

			for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
				this.writeGauge(jsonOut, entry.getKey(), ((Gauge<Number>)entry.getValue()).getValue(), timestamp);
			}

			for (Map.Entry<String, Counter> entry : counters.entrySet()) {
				this.writeCounter(jsonOut, entry.getKey(), entry.getValue().getCount(), timestamp);
			}

			for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
				this.writeSampling(jsonOut, entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Meter> entry : meters.entrySet()) {
				this.writeMetered(jsonOut, entry.getKey(), entry.getValue(), timestamp);
			}

			for (Map.Entry<String, Timer> entry : timers.entrySet()) {
				this.writeMetered(jsonOut, entry.getKey(), entry.getValue(), timestamp);
				this.writeSampling(jsonOut, entry.getKey(), entry.getValue(), timestamp);
			}
		
			jsonOut.writeEndArray();
			jsonOut.writeEndObject();
			jsonOut.flush();
			request.send();
		} catch (Exception exception) {
			LOGGER.error("Could not prepare request", exception);
		}
	}
	
    @Override public void stop() {
		try {
			this.transport.shutdown();
		} catch (Exception exception) {
			LOGGER.error("Error while trying to shutdown the HttpTransport");
		}
		super.stop();
    }

}
