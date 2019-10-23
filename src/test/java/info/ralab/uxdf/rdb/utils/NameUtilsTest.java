package info.ralab.uxdf.rdb.utils;

import org.junit.Assert;
import org.junit.Test;

public class NameUtilsTest {

    @Test
    public void testCamelToUnderline() {
        String camelString = "oneTwoThreeFour";
        String underString = "one_two_three_four";

        Assert.assertEquals(underString, NameUtils.camelToUnderline(camelString));
    }
}
