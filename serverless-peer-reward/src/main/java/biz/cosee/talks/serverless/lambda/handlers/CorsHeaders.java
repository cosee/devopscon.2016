package biz.cosee.talks.serverless.lambda.handlers;

import java.util.HashMap;
import java.util.Map;

public class CorsHeaders {
    public static Map<String, String> build() {
        Map<String, String> corsHeaders = new HashMap<>();
        corsHeaders.put("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token");
        corsHeaders.put("Access-Control-Allow-Methods", "OPTIONS,ANY");
        corsHeaders.put("Access-Control-Allow-Origin", "*");
        return corsHeaders;
    }
}
