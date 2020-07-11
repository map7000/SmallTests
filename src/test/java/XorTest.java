import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class XorTest {
    public static boolean xor(boolean a, boolean b){
        return a ^ b;
    }

    @Test
    @DisplayName("True XOR True")
    void trueXORtrue(){
        Assertions.assertFalse(xor(true, true));
    }

    @Test
    @DisplayName("True XOR False")
    void trueXORfalse(){
        Assertions.assertTrue(xor(true, false));
    }

    @Test
    @DisplayName("False XOR True")
    void falseXORtrue(){
        Assertions.assertTrue(xor(false, true));
    }

    @Test
    @DisplayName("False XOR False")
    void simple(){
        Assertions.assertFalse(xor(false, false));
    }
}
