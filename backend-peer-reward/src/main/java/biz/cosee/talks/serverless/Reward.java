package biz.cosee.talks.serverless;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Reward {

    // random id
    private String id;

    // range key
    private Long ts;

    private String givenByUsername;
    private String rewardedUser;

    private String description;
    private int amount;

}
