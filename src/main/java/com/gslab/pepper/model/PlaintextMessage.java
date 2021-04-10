package com.gslab.pepper.model;

public class PlaintextMessage {
    String headers;
    String payload;

    public PlaintextMessage(String headers, String payload) {
        this.headers = headers;
        this.payload = payload;
    }

    public String getHeaders() {
        return headers;
    }

    public void setHeaders(String headers) {
        this.headers = headers;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "PlaintextMessage{" +
                "headers='" + headers + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
