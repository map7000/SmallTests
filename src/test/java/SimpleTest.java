import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTest {
    private static Logger logger = LoggerFactory.getLogger(SimpleTest.class);
    @Test
    void simple(){
        String s = 2+2+" = value";
        logger.debug(s);
    }
}
