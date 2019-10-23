package info.ralab.uxdf.rdb;


import info.ralab.uxdf.rdb.model.Table;
import info.ralab.uxdf.rdb.utils.NameUtils;

import java.util.List;

/**
 * <p>生成sql语句时的命名转换策略类</p>
 * <p>
 * Copyright: Copyright (c) 2017
 * Company:
 * NameStrategy.java
 *
 * @author pengqin
 * @version 1.0
 */
interface NameStrategy {
    /**
     * 节点名称转表名称
     *
     * @param nodeName Node名称
     * @return 表名
     */
    String nodeToTableName(String nodeName);

    /**
     * 节点名称转表主键主键名称
     *
     * @param nodeName Node名称
     * @return 表主键名
     */
    String nodeToTablePkName(String nodeName);

    /**
     * 关系名称转中间表名称
     *
     * @param eventName Event名称
     * @return 表名
     */
    String eventToTableName(String eventName, String leftName, String rightName);

    /**
     * 关系名称转中间表主键名称
     *
     * @param eventName Event名称
     * @return 表主键名
     */
    String eventToTablePkName(String eventName, String leftName, String rightName);

    /**
     * 公共属性名称转列名称
     *
     * @param attrName 属性名
     * @return 列名
     */
    String attrToColumnName(String attrName);

    /**
     * 特有属性名称转列名称
     *
     * @param propName 属性名
     * @return 列名
     */
    String propToColumnName(String propName);

    /**
     * 冗余属性名称转列名称
     *
     * @param propName
     * @return
     */
    String redundancyPropToColumnName(String propName);

    /**
     * 转数据库索引名
     *
     * @param tableName 表名
     * @return 索引名
     */
    String indexNameOfTable(String tableName);

    /**
     * 表名转触发器名
     *
     * @param tableName 表名
     * @return 触发器名
     */
    String triggerNameOfTable(String tableName, String keyName);

    /**
     * 名称检测
     *
     * @param tables 表名集合
     */
    void checkName(List<Table> tables);
}
