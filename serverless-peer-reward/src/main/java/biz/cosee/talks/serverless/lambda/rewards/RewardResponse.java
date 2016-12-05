package biz.cosee.talks.serverless.lambda.rewards;

import biz.cosee.talks.serverless.Reward;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RewardResponse {

    private List<Reward> rewards;
    private Long ts;
}
