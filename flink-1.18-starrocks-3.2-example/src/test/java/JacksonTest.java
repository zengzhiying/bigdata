import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.User;
import org.junit.Test;

public class JacksonTest {
    @Test
    public void jacksonSerialization() throws JsonProcessingException {
        User u = new User(Integer.valueOf(1), "xm", Short.valueOf((short) 89), "yes");
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(u);
        System.out.println(json);

        User u1 = mapper.readValue(json, User.class);
        System.out.println(u1.toString());
    }
}
