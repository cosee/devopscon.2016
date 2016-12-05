package biz.cosee.talks.serverless.lambda.generated;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class LambdaProxyResponse {

    private int statusCode;
    private Map<String, String> headers;
    private String body;

}
