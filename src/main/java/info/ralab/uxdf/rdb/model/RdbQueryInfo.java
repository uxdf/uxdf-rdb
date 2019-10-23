package info.ralab.uxdf.rdb.model;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import info.ralab.uxdf.definition.SdEventDefinition;
import info.ralab.uxdf.definition.SdNodeDefinition;
import info.ralab.uxdf.model.SdDataQueryOrder;
import lombok.Data;

import java.util.List;

@Data
public class RdbQueryInfo {
    /**
     * 是否Node实例
     */
    private boolean nodeEntity;
    /**
     * 是否Event实例
     */
    private boolean eventEntity;
    /**
     * Node定义
     */
    private SdNodeDefinition nodeDefinition;
    /**
     * Event定义
     */
    private SdEventDefinition eventDefinition;
    /**
     * ID列的别名
     */
    private String idAlias;
    /**
     * 左关联Node ID的别名
     */
    private String leftAlias;
    /**
     * 右关联Node ID的别名
     */
    private String rightAlias;
    /**
     * 别名
     */
    private String alias;
    /**
     * 表名
     */
    private String table;
    /**
     * 查询串标签
     */
    private String label;
    /**
     * 返回的列
     */
    private BiMap<String, String> columns = HashBiMap.create();
    /**
     * 过滤条件
     */
    private List<RdbQueryParam> params = Lists.newArrayList();
    /**
     * 关联条件
     */
    private RdbJoinInfo join;
    /**
     * Exists条件
     */
    private RdbExistsInfo exists;


    private List<SdDataQueryOrder> orders = Lists.newArrayList();

    /**
     * 是否使用Exists条件
     */
    private boolean useExists = true;

    public void setNodeEntity(final boolean isNodeEntity) {
        this.nodeEntity = isNodeEntity;
        this.eventEntity = !this.nodeEntity;
    }

    public void setEventEntity(final boolean isEventEntity) {
        this.eventEntity = isEventEntity;
        this.nodeEntity = !this.eventEntity;
    }

    public void setExists(final RdbExistsInfo exists) {
        this.exists = exists;
        this.useExists = false;
    }

    public void addExists(final RdbExistsInfo exists) {
        if (this.exists == null) {
            this.exists = new RdbExistsInfo();
        }
        this.exists.getExistsList().add(exists);
    }

    public boolean hasExists() {
        return this.exists != null;
    }
}
