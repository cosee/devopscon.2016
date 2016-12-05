package biz.cosee.talks.serverless.lambda.rewards;

import biz.cosee.talks.serverless.Reward;
import com.amazonaws.util.json.Jackson;
import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class RewardResponseTest {

    private static final RewardResponse EXAMPLE_REWARD_RESPONSE = RewardResponse.builder()
            .ts(1L)
            .rewards(
                    singletonList(Reward.builder()
                            .id("someId")
                            .amount(100)
                            .description("testDescription")
                            .givenByUsername("A")
                            .rewardedUser("B")
                            .ts(1L).build()
                    )
            ).build();

    private static final String REWARD_RESPONSE = "{\"rewards\":[{\"id\":\"someId\",\"ts\":1,\"givenByUsername\":\"A\",\"rewardedUser\":\"B\",\"description\":\"testDescription\",\"amount\":100}],\"ts\":1}";

    @Test
    public void testSerialization() {
        String serializedJson = Jackson.toJsonString(EXAMPLE_REWARD_RESPONSE);
        assertThat(serializedJson).isEqualTo(REWARD_RESPONSE);
    }

    @Test
    public void testDeserialization() {
        RewardResponse rewardResponse = Jackson.fromJsonString(REWARD_RESPONSE, RewardResponse.class);
        assertThat(rewardResponse).isEqualTo(EXAMPLE_REWARD_RESPONSE);
    }


}