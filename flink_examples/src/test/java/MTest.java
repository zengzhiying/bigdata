import com.alibaba.fastjson.JSON;
import com.monchickey.examples.sourcesink.ImageLabel;

public class MTest {
    public static void main(String[] args) {
        if(JSON.isValid("{\"x\": 0.3}")) {
            ImageLabel il = JSON.parseObject(" ", ImageLabel.class);
            System.out.println(il);
        } else {
            System.out.println("校验失败.");
        }
    }
}
