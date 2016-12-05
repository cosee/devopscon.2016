package biz.cosee.talks.serverless.lambda.users;

import biz.cosee.talks.serverless.User;
import biz.cosee.talks.serverless.dynamodb.DynamodbAdapter;
import biz.cosee.talks.serverless.lambda.generated.LambdaProxyRequest;
import biz.cosee.talks.serverless.lambda.generated.LambdaProxyResponse;
import biz.cosee.talks.serverless.lambda.handlers.CorsHeaders;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.util.json.Jackson;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UserHandler implements RequestHandler<LambdaProxyRequest, LambdaProxyResponse> {

    private final DynamodbAdapter dynamodbAdapter;

    public UserHandler() throws InterruptedException {
        val client = AmazonDynamoDBClientBuilder.defaultClient();

        // TODO: Error Handling
        val tablePrefix = System.getenv("TABLE_PREFIX");

        dynamodbAdapter = new DynamodbAdapter(client, tablePrefix);
        this.dynamodbAdapter.checkAndCreateTables();
    }

    @Override
    public LambdaProxyResponse handleRequest(LambdaProxyRequest request, Context context) {
        final Map<String, String> query = request.getQueryStringParameters();

        final List<User> results;
        if (query != null && query.containsKey("usernames")) {
            List<User> usersForQuery = extractUsernamesFromQuery(query);
            results = dynamodbAdapter.retrieveUsers(usersForQuery);
        } else {
            results = dynamodbAdapter.retrieveUsers();
        }

        return wrapInResponse(results);
    }

    private List<User> extractUsernamesFromQuery(Map<String, String> query) {
        String usernames = query.get("usernames");
        return Stream.of(usernames.split(","))
                .map(s -> User.builder().username(s).build())
                .collect(Collectors.toList());
    }

    private LambdaProxyResponse wrapInResponse(List<User> users) {
        UserResponse userResponse = UserResponse.builder().users(users).build();

        LambdaProxyResponse response = LambdaProxyResponse.builder()
                .statusCode(200)
                .body(Jackson.toJsonString(userResponse))
                .headers(CorsHeaders.build())
                .build();
        return response;
    }

}
