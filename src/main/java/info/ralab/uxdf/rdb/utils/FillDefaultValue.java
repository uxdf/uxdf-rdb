package info.ralab.uxdf.rdb.utils;

import com.alibaba.fastjson.JSONObject;
import info.ralab.uxdf.chain.UXDFChain;
import lombok.Data;

/**
 * 填充默认值
 */
@Data
public class FillDefaultValue {
    public static final String CHAIN = "chain";
    public static final String PROPERTY = "property";

    /**
     * 是否填充默认值
     * @param defaultValue 默认值
     * @return 是否填充
     */
    public static boolean isFillDefaultValue(final Object defaultValue) {
        if (defaultValue instanceof JSONObject) {
            JSONObject fillDefaultValue = (JSONObject) defaultValue;
            return fillDefaultValue.containsKey(CHAIN) && fillDefaultValue.containsKey(PROPERTY);
        } else {
            return false;
        }
    }

    /**
     * 获取填充默认值
     * @param defaultValue 默认值
     * @return 填充默认值
     */
    public static FillDefaultValue getInstance(final Object defaultValue) {
        if (isFillDefaultValue(defaultValue)) {
            JSONObject jsonObject = (JSONObject) defaultValue;
            FillDefaultValue fillDefaultValue = new FillDefaultValue();
            fillDefaultValue.chain = UXDFChain.getInstance(jsonObject.getString(CHAIN));
            fillDefaultValue.property = jsonObject.getString(PROPERTY);
            return fillDefaultValue;
        } else {
            return null;
        }
    }

    private UXDFChain chain;
    private String property;
}
