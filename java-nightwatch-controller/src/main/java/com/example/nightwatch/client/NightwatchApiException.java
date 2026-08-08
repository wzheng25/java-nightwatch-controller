package com.example.nightwatch.client;

import org.springframework.http.HttpStatusCode;

public class NightwatchApiException extends RuntimeException {
    private final HttpStatusCode statusCode;
    private final String responseBody;

    public NightwatchApiException(HttpStatusCode statusCode, String responseBody) {
        super("Nightwatch API returned HTTP " + statusCode.value());
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public HttpStatusCode statusCode() {
        return statusCode;
    }

    public String responseBody() {
        return responseBody;
    }

    public boolean isTransient() {
        int code = statusCode.value();
        return code == 429 || code == 500 || code == 502 || code == 503 || code == 504;
    }
}
