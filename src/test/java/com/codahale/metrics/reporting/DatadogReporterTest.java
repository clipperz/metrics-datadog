package com.codahale.metrics.reporting;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class DatadogReporterTest {
	
	MetricRegistry registry;
	MockTransport transport;
	String host;
	Clock clock;
	DatadogReporter reporter;

	public DatadogReporterTest() {}
	
	@BeforeClass
	public static void setUpClass() {}
	
	@AfterClass
	public static void tearDownClass() {}
	
	@Before
	public void setUp() {
		registry = new MetricRegistry();
		transport = new MockTransport();
		host = "tests";
		clock = Clock.defaultClock();
		reporter = DatadogReporter.forRegistry(registry)
						.convertRatesTo(TimeUnit.SECONDS)
						.convertDurationsTo(TimeUnit.MILLISECONDS)
						.build(transport, host);
	}
	
	@After
	public void tearDown() {}

//	@SuppressWarnings("unchecked")
	@Test
	public void testBasicSend() throws JsonParseException, JsonMappingException, IOException {
		Counter counter = registry.counter(MetricRegistry.name(DatadogReporterTest.class, "my.counter"));
		counter.inc();

		registry.register(MetricRegistry.name(DatadogReporterTest.class, "my.invocations"), new Gauge<Long>() {
			private long numInovcations = 123;

			@Override
			public Long getValue() {
				return numInovcations++;
			}
		});

		assertEquals(0, transport.numRequests);
		reporter.report();
		assertEquals(1, transport.numRequests);

		String body = new String(transport.lastRequest.getPostBody(), "UTF-8");
		Map<String, Object> request = new ObjectMapper().readValue(body, HashMap.class);

		assertEquals(1, request.keySet().size());
		List<Object> series = (List<Object>) request.get("series");

		assertEquals(2, series.size());
		Map<String, Object> gaugeEntry = (Map<String, Object>) series.get(0);
		Map<String, Object> counterEntry = (Map<String, Object>) series.get(1);

		assertEquals("com.codahale.metrics.reporting.DatadogReporterTest.my.counter", counterEntry.get("metric"));
		assertEquals("counter", counterEntry.get("type"));
		List<List<Number>> points = (List<List<Number>>) counterEntry.get("points");
		assertEquals(1, points.get(0).get(1));

		assertEquals("com.codahale.metrics.reporting.DatadogReporterTest.my.invocations", gaugeEntry.get("metric"));
		assertEquals("gauge", gaugeEntry.get("type"));
		points = (List<List<Number>>) gaugeEntry.get("points");
		assertEquals(123, points.get(0).get(1));
	}

	@Test
	public void testSupplyHostname() throws UnsupportedEncodingException {
		Counter counter = registry.counter(MetricRegistry.name(DatadogReporterTest.class, "my.counter"));
		counter.inc();

		assertEquals(0, transport.numRequests);
		reporter.report();
		assertEquals(1, transport.numRequests);
		String body = new String(transport.lastRequest.getPostBody(), "UTF-8");

		assertFalse(body.indexOf("\"host\":\"hostname\"") > -1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testTaggedMeter() throws Throwable {
		Meter s = registry.meter(MetricRegistry.name(String.class, "meter[with,tags]", "ticks"));
		s.mark();

		reporter.report();
		String body = new String(transport.lastRequest.getPostBody(), "UTF-8");

		Map<String, Object> request = new ObjectMapper().readValue(body, HashMap.class);
		List<Object> series = (List<Object>) request.get("series");

		for (Object o : series) {
			HashMap<String, Object> rec = (HashMap<String, Object>) o;
			List<String> tags = (List<String>) rec.get("tags");
			String name = rec.get("metric").toString();

			assertTrue(name.startsWith("java.lang.String.meter"));
			assertEquals("with", tags.get(0));
			assertEquals("tags", tags.get(1));
		}
	}
}
