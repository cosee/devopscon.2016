package biz.cosee.talks.serverless.dynamodb;

import biz.cosee.talks.serverless.Reward;
import biz.cosee.talks.serverless.User;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import com.amazonaws.services.dynamodbv2.xspec.ScanExpressionSpec;
import com.amazonaws.services.dynamodbv2.xspec.UpdateItemExpressionSpec;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.StreamSupport;

import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.N;
import static com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder.S;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@Slf4j
public class DynamodbAdapter {

    private final DynamoDB dynamodb;

    private final String userTableName;
    private final String rewardTableName;

    private interface UserTable {
        String USERNAME = "username";
        String REWARDS = "totalRewards";
        String REWARDS_FOR_PREFIX = "rewardsFor:";
    }

    private interface RewardTable {
        String ID = "id";
        String TS = "ts";
        String AMOUNT = "amount";
        String FROM_USER = "fromUser";
        String TO_USER = "toUser";
        String DESCRIPTION = "description";
    }

    public DynamodbAdapter(AmazonDynamoDB amazonDynamoDB, String tablePrefix) {
        this.dynamodb = new DynamoDB(amazonDynamoDB);
        this.userTableName = tablePrefix + "-users";
        this.rewardTableName = tablePrefix + "-rewards";
    }

    public void checkAndCreateTables() throws InterruptedException {
        try {
            dynamodb.getTable(rewardTableName).describe();
        } catch (ResourceNotFoundException rnfe) {
            dynamodb.createTable(new CreateTableRequest().withTableName(rewardTableName)
                    .withKeySchema(
                            new KeySchemaElement(RewardTable.ID, KeyType.HASH),
                            new KeySchemaElement(RewardTable.TS, KeyType.RANGE))
                    .withAttributeDefinitions(
                            new AttributeDefinition(RewardTable.ID, ScalarAttributeType.S),
                            new AttributeDefinition(RewardTable.TS, ScalarAttributeType.N)
                    )
                    .withProvisionedThroughput(new ProvisionedThroughput(100L, 100L))
            ).waitForActive();
        }

        try {
            dynamodb.getTable(userTableName).describe();
        } catch (ResourceNotFoundException rnfe) {
            dynamodb.createTable(new CreateTableRequest()
                    .withTableName(userTableName)
                    .withKeySchema(
                            new KeySchemaElement(UserTable.USERNAME, KeyType.HASH)
                    ).withAttributeDefinitions(
                            new AttributeDefinition(UserTable.USERNAME, ScalarAttributeType.S)
                    ).withProvisionedThroughput(new ProvisionedThroughput(100L, 100L))
            ).waitForActive();
        }
    }

    public void deleteTables() throws InterruptedException {
        Table rewardTable = dynamodb.getTable(rewardTableName);
        rewardTable.delete();
        rewardTable.waitForDelete();

        Table userTable = dynamodb.getTable(userTableName);
        userTable.delete();
        userTable.waitForDelete();
    }

    public void rewardUser(Reward reward) {
        saveRewardToStream(reward);
        saveThatUserGaveAReward(reward);
        saveThatUserReceivedAReward(reward);
    }

    private void saveThatUserGaveAReward(Reward reward) {
        Table userTable = dynamodb.getTable(userTableName);

        UpdateItemExpressionSpec countRewardsToUsers = new ExpressionSpecBuilder()
                .addUpdate(
                        N(UserTable.REWARDS_FOR_PREFIX + reward.getRewardedUser()).add(reward.getAmount()))
                .buildForUpdate();

        userTable.updateItem(new UpdateItemSpec()
                .withPrimaryKey(UserTable.USERNAME, reward.getGivenByUsername())
                .withExpressionSpec(countRewardsToUsers));
    }

    private void saveThatUserReceivedAReward(Reward reward) {
        Table userTable = dynamodb.getTable(userTableName);

        UpdateItemExpressionSpec rewardUserSpec = new ExpressionSpecBuilder()
                .addUpdate(
                        N(UserTable.REWARDS).add(reward.getAmount()))
                .buildForUpdate();

        userTable.updateItem(new UpdateItemSpec()
                .withPrimaryKey(UserTable.USERNAME, reward.getRewardedUser())
                .withExpressionSpec(rewardUserSpec));
    }

    private void saveRewardToStream(Reward reward) {
        Table rewards = dynamodb.getTable(rewardTableName);
        String id = LocalDate.now(ZoneOffset.UTC).toString();

        rewards.putItem(new PutItemSpec().withItem(new Item()
                .withString(RewardTable.ID, id)
                .withNumber(RewardTable.TS, reward.getTs())
                .withNumber(RewardTable.AMOUNT, reward.getAmount())
                .withString(RewardTable.FROM_USER, reward.getGivenByUsername())
                .withString(RewardTable.TO_USER, reward.getRewardedUser())
                .withString(RewardTable.DESCRIPTION, reward.getDescription())));
    }


    public List<Reward> retrieveRewardsSince(Long epochMillis) {
        ScanExpressionSpec scanSpec = new ExpressionSpecBuilder()
                .withCondition(N(RewardTable.TS).ge(epochMillis))
                .buildForScan();

        Table rewardTable = dynamodb.getTable(rewardTableName);
        ItemCollection<ScanOutcome> pages = rewardTable.scan(new ScanSpec()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExpressionSpec(scanSpec));

        List<Reward> rewards = StreamSupport.stream(pages.spliterator(), false)
                .map(this::itemToReward).collect(toList());

        log.info("Rewards since {}. Consumed Capacity: {}", epochMillis, pages.getAccumulatedConsumedCapacity());


        return rewards;
    }

    public List<Reward> retrieveRewardsLast(int count) {
        val querySpec = new ExpressionSpecBuilder()
                .withKeyCondition(S(RewardTable.ID).eq(LocalDate.now(ZoneOffset.UTC).toString()))
                .buildForQuery();

        Table rewardTable = dynamodb.getTable(rewardTableName);
        ItemCollection<QueryOutcome> pages = rewardTable.query(new QuerySpec()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExpressionSpec(querySpec)
                .withScanIndexForward(false)
                .withMaxPageSize(count)
        );

        val rewards = StreamSupport.stream(pages.firstPage().spliterator(), false)
                .map(this::itemToReward)
                .collect(toList());

        log.info("Rewards count {}. Consumed Capacity: {}", count, pages.getAccumulatedConsumedCapacity());


        return rewards;
    }

    private Reward itemToReward(Item item) {
        return Reward.builder()
                .id(item.getString(RewardTable.ID))
                .ts(item.getLong(RewardTable.TS))
                .givenByUsername(item.getString(RewardTable.FROM_USER))
                .rewardedUser(item.getString(RewardTable.TO_USER))
                .amount(item.getInt(RewardTable.AMOUNT))
                .description(item.getString(RewardTable.DESCRIPTION))
                .build();
    }

    public List<User> retrieveUsers(Collection<User> users) {
        val userTableKeysAndAttributes = new TableKeysAndAttributes(userTableName);
        users.forEach(u -> userTableKeysAndAttributes.addHashOnlyPrimaryKey(UserTable.USERNAME, u.getUsername()));

        return dynamodb.batchGetItem(userTableKeysAndAttributes).getTableItems().get(userTableName).stream()
                .map(this::itemToUser)
                .collect(toList());
    }

    private User itemToUser(Item i) {
        return User.builder()
                .username(i.getString(UserTable.USERNAME))
                .rewards(i.isPresent(UserTable.REWARDS) ? i.getInt(UserTable.REWARDS) : 0)
                .coinsGiven(
                        i.asMap().keySet()
                                .stream()
                                .filter(key -> key.startsWith(UserTable.REWARDS_FOR_PREFIX))
                                .collect(toMap(
                                        k -> k.split(":")[1],
                                        i::getInt
                                ))
                ).build();
    }

    public List<User> retrieveUsers() {
        Table userTable = dynamodb.getTable(userTableName);
        ItemCollection<ScanOutcome> pages = userTable.scan(new ScanSpec()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL));

        List<User> users = StreamSupport.stream(pages.spliterator(), false)
                .map(this::itemToUser)
                .collect(toList());

        log.info("Users count {}. Consumed Capacity: {}", users.size(), pages.getAccumulatedConsumedCapacity());

        return users;
    }

    public List<Reward> retrieveRewardsContaining(User user) {
        val querySpec = new ExpressionSpecBuilder()
                .withKeyCondition(S(RewardTable.ID).eq(LocalDate.now(ZoneOffset.UTC).toString()))
                .buildForQuery();

        val rewardTable = dynamodb.getTable(rewardTableName);
        ItemCollection<QueryOutcome> pages = rewardTable.query(new QuerySpec()
                .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .withExpressionSpec(querySpec)
        );

        val rewards = StreamSupport.stream(pages.spliterator(), false)
                .map(this::itemToReward)
                .filter(r -> r.getGivenByUsername().equals(user.getUsername()) || r.getRewardedUser().equals(user.getUsername()))
                .collect(toList());

        log.info("Rewards count {}. Consumed Capacity: {}", rewards, pages.getAccumulatedConsumedCapacity());

        Collections.reverse(rewards);
        return rewards;
    }

}
