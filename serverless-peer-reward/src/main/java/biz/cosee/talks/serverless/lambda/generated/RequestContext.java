
package biz.cosee.talks.serverless.lambda.generated;

import lombok.Data;

import java.util.Map;

@Data
public class RequestContext {

    public String accountId;
    public String resourceId;
    public String stage;
    public String requestId;
    public Map<String, String> identity;
    public String resourcePath;
    public String httpMethod;
    public String apiId;

}
