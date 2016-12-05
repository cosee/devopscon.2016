package biz.cosee.talks.serverless;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private String username;
    private int rewards;
    private Map<String, Integer> coinsGiven;
}
