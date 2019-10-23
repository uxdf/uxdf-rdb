package info.ralab.uxdf.rdb.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.util.DigestUtils;

/**
 * <p>数据库列定义</p>
 * Copyright: Copyright (c) 2017
 * Company:
 * Column.java
 *
 * @author pengqin
 * @version 1.0
 * @date 2017年11月15日
 */
@Data
@AllArgsConstructor(staticName = "of")
public class Column {

    public static final String ID_NAME = "__id";
    /**
     * 对应的属性名称
     */
    private String fieldName;
    /**
     * 列名
     */
    private String name;
    /**
     * 列类型
     */
    private String type;
    /**
     * 是否主键
     */
    private Boolean isKey;
    /**
     * 是否可控
     */
    private Boolean isNullable;
    /**
     * 列注释
     */
    private String comment;
    /**
     * 长度
     */
    private Long length;
    /**
     * 精度
     */
    private Integer precision;
    /**
     * 是否索引
     */
    private Boolean isIndex;
    /**
     * 索引名称
     */
    private String indexName;
    /**
     * 是否唯一索引
     */
    private Boolean isUniqueIndex;

    /**
     * 生成Column构造器
     *
     * @return 列构造器
     */
    public static Builder newColumn() {
        return new Builder();
    }

    /**
     * 数据库列构造器
     */
    public static class Builder {

        private String fieldName;
        private String name;
        private String type;
        private Boolean isKey = false;
        private Boolean isNullable = false;
        private Boolean isUniqueIndex = false;
        private Boolean isIndex = false;
        private String indexName;
        private String comment;
        private Long length;
        private Integer precision;

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder type(String type) {
            this.type = type;
            return this;
        }

        public Builder isKey(Boolean isKey) {
            this.isKey = isKey;
            return this;
        }

        public Builder nullable(Boolean nullable) {
            this.isNullable = nullable;
            return this;
        }

        public Builder isIndex(Boolean isIndex, String tableName) {
            this.isIndex = isIndex;
            if (isIndex) {
                // 生成索引名称
                this.indexName = "I_" +
                        DigestUtils.md5DigestAsHex((tableName + this.name).getBytes())
                                .toUpperCase().substring(8, 24);
            }
            return this;
        }

        public Builder isUniqueIndex(Boolean isUniqueIndex) {
            this.isUniqueIndex = isUniqueIndex;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder length(Long length) {
            this.length = length;
            return this;
        }

        public Builder precision(Integer precision) {
            this.precision = precision;
            return this;
        }

        /**
         * 生成列信息{@link Column}
         *
         * @return 列信息
         */
        public Column build() {
            return Column.of(
                    fieldName,
                    name,
                    type,
                    isKey,
                    isNullable,
                    comment,
                    length,
                    precision,
                    isIndex,
                    indexName,
                    isUniqueIndex
            );
        }
    }
}
