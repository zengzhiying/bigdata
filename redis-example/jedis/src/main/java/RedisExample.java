import java.util.HashMap;

public class RedisExample {

    public static void main(String[] args) {
        RedisUtil.setStringValue("foo", "123");
        System.out.println(RedisUtil.getStringValue("foo"));

        HashMap<String, String> m = new HashMap<>();
        m.put("sub1", "1");
        m.put("sub2", "2");
        RedisUtil.setHashAll("m", m);
        System.out.println(RedisUtil.getHashAll("m"));


        RedisUtil.setHashValue("m", "main", "do");
        System.out.println(RedisUtil.getHashByKey("m", "main"));

        RedisUtil.close();
    }

}
