package info.ralab.uxdf.rdb.model;

import com.google.common.collect.Lists;
import info.ralab.uxdf.definition.SdDefinition;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * <p>数据库表定义实体</p>
 * Copyright: Copyright (c) 2017
 * Company:
 * Table.java
 *
 * @author pengqin
 * @version 1.0
 */
@Data
@AllArgsConstructor(staticName = "of")
public class Table {

    public static final String DEFAULT_KEY_NAME = "A_ID";
    /**
     * 表名
     */
    private String name;
    /**
     * 主键名称
     */
    private String pkName;
    /**
     * 索引名
     */
    private String indexName;
    /**
     * 序列名
     */
    private String seqName;
    /**
     * 表说明
     */
    private String comment;
    /**
     * 表明列集合
     */
    private List<Column> columns;
    /**
     * 主键列集合
     */
    private List<String> keys;
    /**
     * Sd定义
     */
    private SdDefinition sdImpl;

    /**
     * 返回主键列
     *
     * @return
     */
    public Column getKey() {
        Optional<Column> op = columns.stream().filter(Column::getIsKey).findFirst();
        return op.isPresent() ? op.get() : null;
    }

    public String getKeyName() {
        return this.getKey() == null ? DEFAULT_KEY_NAME : this.getKey().getName();
    }

    /**
     * 返回唯一索引列
     *
     * @return List<Column>
     */
    public List<Column> getUniqueIndex() {
        return columns.stream().filter(Column::getIsUniqueIndex).collect(Collectors.toList());
    }

    /**
     * 是否包含唯一索引
     *
     * @return Boolean
     */
    public Boolean hasUniqueIndex() {
        List<Column> index = this.getUniqueIndex();
        return index != null && !index.isEmpty();
    }

    public Boolean hasIndex() {
        List<Column> index = this.getIndex();
        return index != null && !index.isEmpty();
    }

    public List<Column> getIndex() {
        return columns.stream().filter(Column::getIsIndex).collect(Collectors.toList());
    }

    /**
     * 创建新的数据库表
     *
     * @return {@link Builder}
     */
    public static Builder newTable(final SdDefinition sdImpl) {
        return new Builder(sdImpl);
    }

    /**
     * 数据库列构造器
     */
    public static class Builder {

        private String name;
        private String pkName;
        private String indexName;
        private String seqName;
        private String comment;

        private List<Column> columns = Lists.newArrayList();
        private List<String> keys = Lists.newArrayList();

        private SdDefinition sdImpl;

        private Builder(final SdDefinition sdImpl) {
            this.sdImpl = sdImpl;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder pkName(String pkName) {
            this.pkName = pkName;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder seqName(String seqName) {
            this.seqName = seqName;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        // 添加列信息
        public Builder column(final Column.Builder builder) {
            Column column = builder.build();
            this.columns.add(column);
            if (column.getIsKey()) {
                this.keys.add(column.getName());
            }
            return this;
        }

        public Table build() {
            return Table.of(name, pkName, indexName, seqName, comment, columns, keys, sdImpl);
        }

    }
}
