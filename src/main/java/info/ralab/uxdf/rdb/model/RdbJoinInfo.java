package info.ralab.uxdf.rdb.model;

import com.google.common.collect.Maps;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

import static lombok.AccessLevel.NONE;

@Data
public class RdbJoinInfo {

    public static final String TYPE_INNER = "INNER";
    public static final String TYPE_LEFT = "LEFT";
    public static final String TYPE_RIGHT = "RIGHT";
    public static final String TYPE_FULL = "FULL";
    public static final String TYPE_DEFAULT = "";

    /**
     * 要关联的集合别名
     */
    private String joinAlias;
    private String joinType = TYPE_DEFAULT;

    @Setter(NONE)
    @Getter
    private Map<String, String> joinColumns = Maps.newHashMap();

    /**
     * 添加关联列映射
     *
     * @param self 当前表的列
     * @param join 关联表的列
     */
    public void putJoinColumn(final String self, final String join) {
        this.joinColumns.put(self, join);
    }

}
