package com.codahale.metrics.reporting;

import com.codahale.metrics.Timer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.CharBuffer;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.protocol.HttpContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTransport implements Transport {
	protected	final CloseableHttpAsyncClient	client;
	protected	final String					targetUrl;
	protected	final Timer						sendTimer;

	private static final Logger logger = LoggerFactory.getLogger(HttpTransport.class);

	public HttpTransport(String host, String apiKey, Timer sendTimer) {
		this.targetUrl = String.format("https://%s/api/v1/series?api_key=%s", host, apiKey);
		this.sendTimer = sendTimer;
		this.client = HttpAsyncClients.createDefault();
		this.client.start();
	}

	public HttpTransport(String host, String apiKey) {
		this(host, apiKey, null);
	}
	
	@Override public HttpRequest prepare() throws IOException {
		return new HttpRequest(this);
	}

	public void shutdown () throws IOException {
		this.client.close();
	}

	//==========================================================================

	public static class HttpRequest implements Transport.Request {
		protected final HttpTransport			transport;
		protected final ByteArrayOutputStream	requestBodyWriter;
		protected final Timer.Context			context;

		public HttpRequest(HttpTransport transport) {
			this.transport = transport;
			this.requestBodyWriter = new ByteArrayOutputStream();
			if (this.transport.sendTimer != null) {
				this.context = this.transport.sendTimer.time();
			} else {
				this.context = null;
			}
		}

		@Override public OutputStream getBodyWriter() {
			return requestBodyWriter;
		}

		@Override public void send() throws Exception {
			HttpEntity						requestEntity;
			HttpPost						request;
			HttpAsyncRequestProducer		requestProducer;
			AsyncCharConsumer<HttpResponse>	responseConsumer;

			requestBodyWriter.flush();
			requestBodyWriter.close();
			
			requestEntity = new ByteArrayEntity(requestBodyWriter.toByteArray(), ContentType.APPLICATION_JSON);
			request = new HttpPost(this.transport.targetUrl);
			request.setEntity(requestEntity);
			
			requestProducer = HttpAsyncMethods.create(request);
			responseConsumer = new AsyncCharConsumer<HttpResponse>() {
				HttpResponse response;
				
				@Override protected void onCharReceived(CharBuffer buf, IOControl ioctrl) throws IOException {
				}

				@Override protected void onResponseReceived(HttpResponse response) throws HttpException, IOException {
					this.response = response;
				}

				@Override protected void releaseResources() {
					if (context != null) {
						context.stop();
					}
				}
				
				@Override protected HttpResponse buildResult(HttpContext context) throws Exception {
					return this.response;
				}
			};
			
			this.transport.client.execute(requestProducer, responseConsumer, new FutureCallback<HttpResponse>() {
				@Override public void completed(HttpResponse result) {
					logger.debug("metrics successfully sent");
				}

				@Override public void failed(Exception exception) {
					logger.error("error while sending metrics", exception);
				}

				@Override public void cancelled() {
					logger.warn("metrics sending cancelled");
				}
			});
		}
	}
}
