
package biz.cosee.talks.serverless.lambda.generated;

import lombok.Data;

import java.util.Map;

@Data
public class LambdaProxyRequest {

    private String resource;
    private String path;
    private String httpMethod;
    private Map<String, String> headers;
    private Map<String, String> queryStringParameters;
    private Map<String, String> pathParameters;
    private Map<String, String> stageVariables;
    private RequestContext requestContext;
    private String body;

}
