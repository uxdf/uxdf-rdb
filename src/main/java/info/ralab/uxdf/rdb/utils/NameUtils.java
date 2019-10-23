package info.ralab.uxdf.rdb.utils;

/**
 * <p>名称转换工具类</p>
 * Copyright: Copyright (c) 2017
 * Company:
 * NameUtils.java
 *
 * @author pengqin
 * @version 1.0
 */
public class NameUtils {

    public static final String UNDER_LINE = "_";

    private NameUtils() {

    }

    /**
     * 驼峰命名转下划线 命名
     *
     * @param name 驼峰命名的变量名称
     * @return 下划线命名的变量名称
     */
    public static String camelToUnderline(final String name) {
        // 空名称直接返回
        if (name == null || name.trim().isEmpty()) {
            return name;
        }
        int len = name.length();
        StringBuilder buf = new StringBuilder(name);
        // 遍历名称
        for (int i = 1; i < len - 1; i++) {
            // 如果当前字符是大写，并且前后都是小写字符，则在当前字符前插入下划线
            if (
                    Character.isLowerCase(buf.charAt(i - 1)) &&
                            Character.isUpperCase(buf.charAt(i)) &&
                            Character.isLowerCase(buf.charAt(i + 1))
            ) {
                buf.insert(i++, UNDER_LINE);
                len = name.length();
            }
        }
        return buf.toString().toLowerCase();
    }

}
