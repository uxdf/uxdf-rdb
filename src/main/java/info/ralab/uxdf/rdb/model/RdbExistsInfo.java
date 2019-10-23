package info.ralab.uxdf.rdb.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

@Data
public class RdbExistsInfo {
    private String existsTable;
    @Setter(AccessLevel.NONE)
    @Getter
    private Map<String, String> existsColumns = Maps.newHashMap();
    private String selfTable;
    private boolean close;
    /**
     * 过滤条件
     */
    private List<RdbQueryParam> params = Lists.newArrayList();

    private List<RdbExistsInfo> existsList = Lists.newArrayList();

    public boolean effective() {
        return StringUtils.isNotBlank(existsTable) && StringUtils.isNotBlank(selfTable);
    }

    /**
     * 添加过滤字段映射
     *
     * @param self   当前表字段
     * @param exists 过滤表字段
     */
    public void putExistsColumn(final String self, final String exists) {
        this.existsColumns.put(self, exists);
    }

    /**
     * 合并子条件，汇总到当前集合中。并且添加结束信息。
     */
    public void mergeChildren() {
        if (existsList.isEmpty()) {
            return;
        }
        List<RdbExistsInfo> mergeList = Lists.newArrayList();

        existsList.forEach(rdbExistsInfo -> {
            // 添加当前
            if (rdbExistsInfo.effective()) {
                mergeList.add(rdbExistsInfo);
            }
            // 添加所有子
            rdbExistsInfo.mergeChildren();
            mergeList.addAll(rdbExistsInfo.getExistsList());

            // 添加结束
            if (rdbExistsInfo.effective()) {
                RdbExistsInfo closeExistsInfo = new RdbExistsInfo();
                closeExistsInfo.setClose(true);
                mergeList.add(closeExistsInfo);
            }
        });

        this.existsList = mergeList;
    }
}
