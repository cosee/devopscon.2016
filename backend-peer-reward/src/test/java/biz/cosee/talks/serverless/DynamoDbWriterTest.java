package biz.cosee.talks.serverless;

import biz.cosee.talks.serverless.dynamodb.DynamodbAdapter;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDbWriterTest {

    private AmazonDynamoDB dynamodb;
    private DynamodbAdapter writer;

    @Before
    public void setUpDynamoDB() throws Exception {
        // uncomment for dynamodb local tests
//         this.dynamodb = new AmazonDynamoDBClient(new BasicAWSCredentials("foobar", "foobar"));
//         this.dynamodb.setEndpoint("http://127.0.0.1:8000");
        dynamodb = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(dynamodb);
        writer = new DynamodbAdapter(dynamodb, "tmp-tests");
        writer.checkAndCreateTables();
    }

    @After
    public void tearDownDynamoDB() throws InterruptedException {
        writer.deleteTables();
    }

    @Test
    public void ensureTableExists() {
        List<String> tableNames = dynamodb.listTables().getTableNames();
        assertThat(tableNames).contains("tmp-tests-rewards");
    }

    @Test
    public void insertAndRetrieve() {
        Reward reward = Reward.builder()
                .givenByUsername("andreas")
                .rewardedUser("alex")
                .amount(100)
                .ts(System.currentTimeMillis())
                .description("placeholder").build();

        writer.rewardUser(reward);
        List<Reward> rewards = writer.retrieveRewardsSince(0L);


        assertThat(rewards).hasSize(1);

        assertThat(rewards.get(0).getId()).isNotEmpty();
        assertThat(rewards.get(0).getGivenByUsername()).isEqualTo(reward.getGivenByUsername());
        assertThat(rewards.get(0).getRewardedUser()).isEqualTo(reward.getRewardedUser());
        assertThat(rewards.get(0).getTs()).isEqualTo(reward.getTs());
        assertThat(rewards.get(0).getAmount()).isEqualTo(reward.getAmount());
        assertOrderedDescending(rewards);

        val users = writer.retrieveUsers(
                Arrays.stream(new String[]{"andreas", "alex"})
                        .map(name -> User.builder().username(name).build())
                        .collect(Collectors.toList())
        );

        assertThat(users.size()).isEqualTo(2);


        val expectedMapAndreas = new HashMap<String, Integer>();

        val storedAndreas = users.stream()
                .filter(u -> u.getUsername().equals("andreas"))
                .findFirst()
                .orElseThrow(NullPointerException::new);

        assertThat(storedAndreas.getRewards()).isEqualTo(0);
        assertThat(storedAndreas.getCoinsGiven().isEmpty()).isFalse();
        assertThat(storedAndreas.getCoinsGiven().get("alex")).isEqualTo(100);

        val storedAlex = users.stream()
                .filter(u -> u.getUsername().equals("alex"))
                .findFirst()
                .orElseThrow(NullPointerException::new);

        assertThat(storedAlex.getRewards()).isEqualTo(100);
        assertThat(storedAlex.getCoinsGiven().isEmpty()).isTrue();
    }

    @Test
    public void whenInsertingRewardsShouldReturnAllMentionedUsers() {
        Arrays.asList(
                Reward.builder()
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(100)
                        .ts(System.currentTimeMillis())
                        .description("placeholder").build(),

                Reward.builder()
                        .givenByUsername("andreas")
                        .rewardedUser("markus")
                        .amount(10)
                        .ts(System.currentTimeMillis())
                        .description("placeholder").build(),

                Reward.builder()
                        .givenByUsername("michi")
                        .rewardedUser("andreas")
                        .amount(10)
                        .ts(System.currentTimeMillis())
                        .description("placeholder").build()
        ).forEach(r -> writer.rewardUser(r));

        val names = writer.retrieveUsers().stream()
                .map(User::getUsername)
                .collect(Collectors.toList());

        val expectedNames = Arrays.asList("andreas", "alex", "markus", "michi");
        assertThat(names).containsAll(expectedNames);
        assertThat(expectedNames).containsAll(names);
    }

    @Test
    public void whenInsertingFiveItemsAndRequestingThreeShouldReturnLastThree() {
        Arrays.stream(new Reward[]{
                Reward.builder()
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(1L)
                        .description("placeholder").build(),
                Reward.builder()
                        .givenByUsername("alex")
                        .rewardedUser("andreas")
                        .amount(10)
                        .ts(2L)
                        .description("placeholder").build(),
                Reward.builder()
                        .givenByUsername("markus")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(3L)
                        .description("placeholder").build(),
                Reward.builder()
                        .givenByUsername("andreas")
                        .rewardedUser("markus")
                        .amount(10)
                        .ts(4L)
                        .description("placeholder").build(),
                Reward.builder()
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(5L)
                        .description("placeholder").build()
        }).forEach(r -> writer.rewardUser(r));


        val expected = Arrays.asList(
                Reward.builder()
                        .id(today())
                        .givenByUsername("markus")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(3L)
                        .description("placeholder").build(),
                Reward.builder()
                        .id(today())
                        .givenByUsername("andreas")
                        .rewardedUser("markus")
                        .amount(10)
                        .ts(4L)
                        .description("placeholder").build(),
                Reward.builder()
                        .id(today())
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(5L)
                        .description("placeholder").build());

        val rewards = writer.retrieveRewardsLast(3);

        assertThat(rewards).containsAll(expected);
        assertThat(expected).containsAll(rewards);
        assertOrderedDescending(rewards);
    }

    private static String today() {
        return LocalDate.now(ZoneOffset.UTC).toString();
    }

    @Test
    public void whenQueryingRewardsWithUserShouldReturnOnlyRewardsWithThatUser() {
        val expectedAndreasRewards = Arrays.asList(
                Reward.builder()
                        .id(today())
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(1L)
                        .description("placeholder").build(),
                Reward.builder()
                        .id(today())
                        .givenByUsername("alex")
                        .rewardedUser("andreas")
                        .amount(10)
                        .ts(2L)
                        .description("placeholder").build(),
                Reward.builder()
                        .id(today())
                        .givenByUsername("andreas")
                        .rewardedUser("markus")
                        .amount(10)
                        .ts(4L)
                        .description("placeholder").build(),
                Reward.builder()
                        .id(today())
                        .givenByUsername("andreas")
                        .rewardedUser("alex")
                        .amount(10)
                        .ts(5L)
                        .description("placeholder").build()
        );

        writer.rewardUser(Reward.builder()
                .givenByUsername("markus")
                .rewardedUser("alex")
                .amount(10)
                .ts(3L)
                .description("placeholder").build());
        expectedAndreasRewards.forEach(r -> writer.rewardUser(r));
        writer.rewardUser(Reward.builder()
                .givenByUsername("miez")
                .rewardedUser("alex")
                .amount(10)
                .ts(6L)
                .description("placeholder").build());

        val andreasRewards = writer.retrieveRewardsContaining(User.builder().username("andreas").build());
        assertThat(expectedAndreasRewards).containsAll(andreasRewards);
        assertThat(andreasRewards).containsAll(expectedAndreasRewards);
        assertOrderedDescending(andreasRewards);
    }

    private void assertOrderedDescending(Collection<Reward> rewards) {
        rewards.stream()
                .map(Reward::getTs)
                .reduce(Long.MAX_VALUE, (acc, cur) -> {
                    if (cur <= acc)
                        return cur;
                    else
                        throw new AssertionError("order is not descending");
                });
    }
}
