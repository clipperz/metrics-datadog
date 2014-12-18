package com.codahale.metrics.reporting;

import com.codahale.metrics.Timer;
import java.io.IOException;

public class MockHttpTransport extends HttpTransport {
	protected	int				numRequests = 0;
	protected	MockHttpRequest	lastRequest;
	
	public MockHttpTransport(Timer sendTimer) {
		super("www.example.com", "key", sendTimer);
	}

	@Override public HttpRequest prepare() throws IOException {
		MockHttpRequest request = new MockHttpRequest(this);
		lastRequest = request;
		numRequests ++;

		return lastRequest;
	}
	
	
	
	public static class MockHttpRequest extends HttpTransport.HttpRequest {
		protected	MockHttpTransport	mockTransport;
		
		public MockHttpRequest(MockHttpTransport transport) {
			super(transport);
			this.mockTransport = transport;
		}

		@Override public void send() throws Exception {
		}

		public byte[] getPostBody() {
			return requestBodyWriter.toByteArray();
		}
		
	}

}
