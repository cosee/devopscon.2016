package biz.cosee.talks.serverless.lambda.users;

import biz.cosee.talks.serverless.User;
import com.amazonaws.util.json.Jackson;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class UserResponseTest {

    private static final String SERIALIZED_USER_RESPONSE = "{\"users\":[{\"username\":\"testUser\",\"rewards\":100,\"coinsGiven\":{\"testUserB\":20}}]}";
    public static final User EXAMPLE_USER = User.builder().username("testUser").rewards(100).coinsGiven(ImmutableMap.of("testUserB", 20)).build();

    @Test
    public void testSerialization() {
        List<User> users = new ArrayList<>();
        users.add(EXAMPLE_USER);
        UserResponse userResponse = UserResponse.builder().users(users).build();
        String serializedJson = Jackson.toJsonString(userResponse);
        assertThat(serializedJson).isEqualTo(SERIALIZED_USER_RESPONSE);
    }

    @Test
    public void testDeserialization() {
        UserResponse parsedResponse = Jackson.fromJsonString(SERIALIZED_USER_RESPONSE, UserResponse.class);
        assertThat(parsedResponse).isNotNull();
        assertThat(parsedResponse.getUsers()).containsExactly(EXAMPLE_USER);
    }




}