package biz.cosee.talks.serverless.lambda.schedule;

import biz.cosee.talks.serverless.Reward;
import biz.cosee.talks.serverless.dynamodb.DynamodbAdapter;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lombok.val;

import java.time.Instant;
import java.util.Random;

public class RandomRewardHandler implements RequestHandler<Void, Void> {

    private final DynamodbAdapter dynamodbAdapter;
    private final static String[] DESCRIPTIONS = new String[] {
            "Dude your code is amazing!",
            "Thanks for helping me out yesterday.",
            "Thanks for finding my Memory leak the other day.",
            "Great advice last week.",
            "You just raise my mood.",
            "Thanks for refactoring my rubbish code.",
            "You're my favorite colleague, because you always write tests.",
            "Had fun playing Mario Kart. Next time I win ;)!",
            "Thanks for reminding me of my meeting.",
            "Your code is always awesome.",
            "I just like you."
    };

    public RandomRewardHandler() throws InterruptedException {
        val client = AmazonDynamoDBClientBuilder.defaultClient();

        // TODO: Error Handling
        val tablePrefix = System.getenv("TABLE_PREFIX");

        dynamodbAdapter = new DynamodbAdapter(client, tablePrefix);
        dynamodbAdapter.checkAndCreateTables();
    }

    @Override
    public Void handleRequest(Void aVoid, Context context) {
        val users = dynamodbAdapter.retrieveUsers();
        if (users.size() < 2)
            throw new RuntimeException("Need at least two users in Database.");

        Random rand = new Random();
        val userFrom = users.get(rand.nextInt(users.size()));
        users.remove(userFrom);
        val userTo = users.get(rand.nextInt(users.size()));
        val amount = (rand.nextInt(5) + 1 ) * 5;
        val description = DESCRIPTIONS[rand.nextInt(DESCRIPTIONS.length)];

        dynamodbAdapter.rewardUser(Reward.builder()
                .amount(amount)
                .description(description)
                .givenByUsername(userFrom.getUsername())
                .rewardedUser(userTo.getUsername())
                .ts(Instant.now().toEpochMilli())
                .build()
        );

        return null;
    }
}
