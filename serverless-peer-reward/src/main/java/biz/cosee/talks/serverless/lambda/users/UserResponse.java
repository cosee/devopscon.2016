package biz.cosee.talks.serverless.lambda.users;


import biz.cosee.talks.serverless.User;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserResponse {

    private List<User> users;

}
