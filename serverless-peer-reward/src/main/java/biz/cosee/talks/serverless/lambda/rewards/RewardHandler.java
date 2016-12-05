package biz.cosee.talks.serverless.lambda.rewards;

import biz.cosee.talks.serverless.Reward;
import biz.cosee.talks.serverless.User;
import biz.cosee.talks.serverless.dynamodb.DynamodbAdapter;
import biz.cosee.talks.serverless.lambda.generated.LambdaProxyRequest;
import biz.cosee.talks.serverless.lambda.generated.LambdaProxyResponse;
import biz.cosee.talks.serverless.lambda.handlers.CorsHeaders;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.json.Jackson;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
public class RewardHandler implements RequestHandler<LambdaProxyRequest, LambdaProxyResponse> {

    private final DynamodbAdapter dynamodbAdapter;

    private final static String GET_PARAMETER_SINCE = "since";
    private final static String GET_PARAMETER_LIMIT = "limit";
    private final static String GET_PARAMETER_USERNAME = "username";

    public RewardHandler() throws InterruptedException {
        val client = AmazonDynamoDBClientBuilder.defaultClient();

        // TODO: Error Handling
        val tablePrefix = System.getenv("TABLE_PREFIX");

        dynamodbAdapter = new DynamodbAdapter(client, tablePrefix);
        dynamodbAdapter.checkAndCreateTables();
    }

    @Override
    public LambdaProxyResponse handleRequest(LambdaProxyRequest request, Context context) {
        String httpMethod = request.getHttpMethod();

        Map<String, String> query = request.getQueryStringParameters();


        switch (httpMethod.toUpperCase()) {
            case "POST":
                if (request.getBody().isEmpty()) {
                    return badRequest();
                }

                Reward reward = Jackson.fromJsonString(request.getBody(), Reward.class);
                reward.setTs(Instant.now().toEpochMilli());
                dynamodbAdapter.rewardUser(reward);
                return LambdaProxyResponse.builder().statusCode(204).headers(CorsHeaders.build()).body("").build();
            case "GET":
                // TODO logging for ambiguous options
                if (query == null) {
                    return badRequest();
                }

                if (query.containsKey(GET_PARAMETER_SINCE)) {
                    List<Reward> rewards = dynamodbAdapter.retrieveRewardsSince(Long.parseLong(query.get(GET_PARAMETER_SINCE)));
                    return wrapInResponse(rewards, Instant.now().toEpochMilli());
                } else if (query.containsKey(GET_PARAMETER_USERNAME)) {
                    List<Reward> rewards = dynamodbAdapter.retrieveRewardsContaining(User.builder().username(query.get(GET_PARAMETER_USERNAME)).build());
                    return wrapInResponse(rewards, Instant.now().toEpochMilli());
                } else if (query.containsKey(GET_PARAMETER_LIMIT)) {
                    List<Reward> rewards = dynamodbAdapter.retrieveRewardsLast(Integer.parseInt(query.get(GET_PARAMETER_LIMIT)));
                    return wrapInResponse(rewards, Instant.now().toEpochMilli());
                }

                break;
            default:
                log.error("Unsupported Methd on RewardHandler: {}", httpMethod.toUpperCase());
        }

        return badRequest();
    }

    private static final LambdaProxyResponse badRequest() {
        return LambdaProxyResponse.builder().statusCode(400).build();
    }


    private LambdaProxyResponse wrapInResponse(List<Reward> rewards, Long ts) {
        RewardResponse userResponse = RewardResponse.builder().rewards(rewards).ts(ts).build();

        LambdaProxyResponse response = LambdaProxyResponse.builder()
                .statusCode(200)
                .body(Jackson.toJsonString(userResponse))
                .headers(CorsHeaders.build())
                .build();
        return response;
    }


}
