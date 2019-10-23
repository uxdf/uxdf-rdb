package info.ralab.uxdf.rdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.Sd;
import info.ralab.uxdf.chain.UXDFChain;
import info.ralab.uxdf.definition.*;
import info.ralab.uxdf.instance.SdEntity;
import info.ralab.uxdf.rdb.exception.TemplateException;
import info.ralab.uxdf.rdb.model.*;
import info.ralab.uxdf.utils.AssociateUniquePropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static info.ralab.uxdf.rdb.model.Column.newColumn;
import static info.ralab.uxdf.rdb.model.Table.newTable;

@Component
@Slf4j
public abstract class SqlGeneratorCommon implements SqlGenerator {
    private static final String SEQ_NODE = "SEQ_NODE";
    private static final String SEQ_EVENT = "SEQ_EVENT";

    /**
     * 默认字符型长度
     */
    private static final Long DEFAULT_LENGTH = 50L;
    /**
     * 超长类型的下限
     */
    private static final Long LARGE_LENGTH = 4000L;

    /**
     * 命名策略
     */
    private NameStrategy nameStrategy;

    /**
     * 模板服务
     */
    private TemplateService templateService;

    @Autowired
    public SqlGeneratorCommon(
            final NameStrategy nameStrategy,
            final TemplateService templateService
    ) {
        this.nameStrategy = nameStrategy;
        this.templateService = templateService;
    }

    /**
     * 获取模板路径，有具体RDB的实现类返回
     *
     * @return 模板路径
     */
    protected abstract String getTemplatePath();

    @Override
    public String generateDDL(final Sd sd, final String... sdNames) {
        // 是否输出序列创建语句
        boolean printSequence = true;

        Sd updateSd = sd;
        // 如果有指定的Sd名称，那么之生成指定的DDL语句
        if (sdNames != null && sdNames.length != 0) {
            // 指定生成时，不输出序列
            printSequence = false;
            updateSd = getUpdateSd(sd, Lists.newArrayList(sdNames));
        }

        // 获取要生成DDL的表集合
        List<Table> tables = loadBySD(updateSd);
        try {
            // 根据数据库定义和表定义生成数据库脚本
            return templateService.resolve(
                    this.getTemplatePath(),
                    ImmutableMap.of(
                            TABLE_KEY,
                            tables,
                            PRINT_SEQUENCE,
                            printSequence
                    )
            );
        } catch (TemplateException e) {
            if (log.isErrorEnabled()) {
                log.error("error when generate ddl {}", e);
            }
            return null;
        }
    }

    /**
     * 基于Sd生成表集合
     *
     * @param sd Sd定义集合
     * @return 要生成的表集合
     */
    private List<Table> loadBySD(final Sd sd) {
        SdNode node = sd.getNode();
        SdEvent event = sd.getEvent();
        List<Table> tables = Lists.newArrayList();

        Map<String, SdProperty> nodeAttr = node.getAttr();
        Map<String, SdNodeDefinition> nodeDefinitions = node.getImpl();
        //遍历IMPL 生成数据库表
        if (nodeDefinitions != null) {
            nodeDefinitions.forEach((name, definition) -> {
                // 元数据表不创建
                if (name.startsWith("$")) {
                    return;
                }
                // 获取表生成器
                Table.Builder builder = newTable(definition);
                // 生成表名称
                String tableName = nameStrategy.nodeToTableName(name);
                // 生成主键名称
                String pkName = nameStrategy.nodeToTablePkName(name);
                // 设置表名、主键名、注释
                builder.name(tableName).pkName(pkName).comment(definition.getTitle());
                //增加公共列
                nodeAttr.forEach((attrName, attr) -> {

                    // 是否主键
                    boolean isKey = SdEntity.ATTR_ID.equals(attrName);

                    // 是否唯一索引
                    boolean isUniqueIndex = SdEntity.ATTR_UUID.equals(attrName);
                    // 是否是索引
                    boolean isIndex = definition.isIndex(attrName);
                    // 列生成器
                    Column.Builder columnBuilder = newColumn()
                            // 属性名
                            .field(attrName)
                            // 列名
                            .name(nameStrategy.attrToColumnName(attrName))
                            // 数据类型
                            .type(attr.getBase().name())
                            // 是否允许为空
                            .nullable(!attr.isRequired())
                            // 注释
                            .comment(attr.getTitle())
                            // 是否主键
                            .isKey(isKey)
                            // 是否索引
                            .isIndex(isIndex, tableName)
                            // 是否唯一索引
                            .isUniqueIndex(isUniqueIndex);

                    // 字符类型需要设置长度
                    if (attr.getBase() == SdBaseType.String) {
                        // 设置长度
                        if (attr.getUpperLimit() == null) {
                            columnBuilder.length(DEFAULT_LENGTH);
                        } else {
                            columnBuilder.length(SdEntity.getBaseInteger(attr.getUpperLimit()));
                        }
                    }
                    builder.column(columnBuilder);
                });

                //增加特定列
                buildProperty(definition, builder, tableName);

                // 构建表信息
                Table table = builder.build();
                // 如果存在唯一索引，创建唯一索引名称
                if (table.hasUniqueIndex()) {
                    table.setIndexName(nameStrategy.indexNameOfTable(table.getName()));
                }
                // 设置序列
                table.setSeqName(SEQ_NODE);

                tables.add(table);
            });
        }

        //遍历EVENT 生成中间表
        Map<String, SdProperty> eventAttrs = event.getAttr();
        Map<String, Map<String, Map<String, SdEventDefinition>>> eventDefinitions = event.getImpl();

        if (eventDefinitions != null) {
            eventDefinitions.forEach((eventName, eventMap) -> {
                // 元数据表不创建
                if (eventName.startsWith("$")) {
                    return;
                }

                //遍历event map
                eventMap.forEach((left, rights) -> {
                    //遍历 event map right 节点
                    rights.forEach((right, definition) -> {
                        Table.Builder builder = newTable(definition);
                        String tableName = nameStrategy.eventToTableName(eventName, left, right);
                        String pkName = nameStrategy.eventToTablePkName(eventName, left, right);
                        // 表名，主键名，注释
                        builder.name(tableName).pkName(pkName).comment(definition.getTitle());

                        eventAttrs.forEach((attrName, attr) -> {
                            // 是否主键
                            boolean isKey = SdEntity.ATTR_ID.equals(attrName);
                            // 是否唯一索引
                            boolean isUniqueIndex = SdEntity.ATTR_UUID.equals(attrName);
                            // 是否是索引
                            boolean isIndex = definition.isIndex(attrName);

                            Column.Builder columnBuilder = newColumn()
                                    // 属性名称
                                    .field(attrName)
                                    // 字段名称
                                    .name(nameStrategy.attrToColumnName(attrName))
                                    // 字段类型
                                    .type(attr.getBase().name())
                                    // 是否主键
                                    .isKey(isKey)
                                    // 是否允许为空
                                    .nullable(!attr.isRequired())
                                    // 是否索引
                                    .isIndex(isIndex, tableName)
                                    // 是否唯一索引
                                    .isUniqueIndex(isUniqueIndex)
                                    // 注释
                                    .comment(attr.getTitle());

                            // 字符类型需要设置长度
                            if (attr.getBase() == SdBaseType.String) {
                                // 设置长度
                                if (attr.getUpperLimit() == null) {
                                    columnBuilder.length(DEFAULT_LENGTH);
                                } else {
                                    columnBuilder.length(SdEntity.getBaseInteger(attr.getUpperLimit()));
                                }
                            }
                            builder.column(columnBuilder);
                        });
                        //增加特定列

                        buildProperty(definition, builder, tableName);

                        Table table = builder.build();
                        if (table.hasUniqueIndex()) {
                            table.setIndexName(nameStrategy.indexNameOfTable(table.getName()));
                        }
                        table.setSeqName(SEQ_EVENT);

                        tables.add(table);
                    });
                });
            });
        }

        //名称检测
        nameStrategy.checkName(tables);
        return tables;
    }

    /**
     * 构建SD定义{@link SdDefinition}中的自定义属性
     *
     * @param sdDefinition Sd定义
     * @param builder      Table构建期
     * @param tableName    Table名称
     */
    private void buildProperty(
            final SdDefinition sdDefinition,
            final Table.Builder builder,
            final String tableName
    ) {
        // 冗余父级id属性
        buildRedundancyProperty(sdDefinition, builder);

        sdDefinition.getProp().forEach((name, sdProperty) -> {

            Column.Builder columnBuilder = newColumn()
                    // 属性名
                    .field(name)
                    // 列名
                    .name(nameStrategy.propToColumnName(name))
                    // 数据类型
                    .type(sdProperty.getBase().name())
                    // 是否允许为空
                    .nullable(!sdProperty.isRequired())
                    // 注释
                    .comment(sdProperty.getTitle())
                    // 是否索引
                    .isIndex(sdDefinition.isIndex(name), tableName);
            // 字符类型需要设置长度
            if (sdProperty.getBase() == SdBaseType.String) {
                // 判断是否超长文本
                Long upperLimit = SdEntity.getBaseInteger(sdProperty.getUpperLimit());
                if (upperLimit == null) {
                    // 未设定，使用默认长度
                    columnBuilder.length(DEFAULT_LENGTH);
                } else if (upperLimit >= 0 && upperLimit < LARGE_LENGTH) {
                    // 在大长度范围内
                    columnBuilder.length(upperLimit);
                } else if (upperLimit >= LARGE_LENGTH) {
                    // 超过大长度，但是有长度限制
                    columnBuilder.type("Clob");
                } else {
                    // 不限制长度
                    columnBuilder.type("Clob");
                }
            } else if (sdProperty.getBase() == SdBaseType.Binary) {
                // 二进制属性，使用BLOG
                columnBuilder.type("Blob");
            }
            builder.column(columnBuilder);

        });
    }

    /**
     * 冗余SD定义{@link SdDefinition}中的父级ID的属性
     *
     * @param sdDefinition Sd定义
     * @param builder      Table构建期
     */
    private void buildRedundancyProperty(
            final SdDefinition sdDefinition,
            final Table.Builder builder
    ) {
        String redundancyPropertyName = AssociateUniquePropertyUtil.getPropertyName(sdDefinition);
        if (redundancyPropertyName != null) {
            Column.Builder columnBuilder = newColumn()
                    // 属性名
                    .field(redundancyPropertyName)
                    // 列名
                    .name(nameStrategy.redundancyPropToColumnName(redundancyPropertyName))
                    // 数据类型
                    .type(SdBaseType.Integer.name())
                    // 是否允许为空
                    .nullable(false);
            builder.column(columnBuilder);
        }
    }

    /**
     * 根据SDNode对象生成节点与数据表的映射关系
     *
     * @param sd Sd定义
     * @return 映射关系
     */
    @Override
    public RelationShipRdb generateRelationShip(final Sd sd) {

        RelationShipRdb relationShip = new RelationShipRdb();

        List<Table> tables = loadBySD(sd);

        tables.forEach(table -> {
            SdDefinition sdImpl = table.getSdImpl();

            RelationShipRdbTable rdbTable = new RelationShipRdbTable();
            rdbTable.setName(table.getName());
            rdbTable.setSeqId(table.getSeqName());
            table.getColumns().forEach(column -> rdbTable.getColumn().put(column.getFieldName(), column.getName()));

            if (sdImpl instanceof SdNodeDefinition) {
                SdNodeDefinition sdNodeImpl = (SdNodeDefinition) sdImpl;
                relationShip.putNodeRdbTable(sdNodeImpl, rdbTable);
            } else if (sdImpl instanceof SdEventDefinition) {
                SdEventDefinition sdEventImpl = (SdEventDefinition) sdImpl;
                relationShip.putEventRdbTable(sdEventImpl, rdbTable);
            }
        });

        return relationShip;
    }

    /**
     * 更新Sd定义集合，移除不在Sd名称集合中的定义。
     *
     * @param sd         Sd定义集合
     * @param sdNameList Sd名称集合
     * @return Sd定义集合
     */
    private Sd getUpdateSd(Sd sd, List<String> sdNameList) {
        //传入的SD
        SdNode node = sd.getNode();
        SdEvent event = sd.getEvent();
        Map<String, SdProperty> nodeAttrs = node.getAttr();
        Map<String, SdNodeDefinition> nodeImpls = node.getImpl();
        Map<String, SdProperty> eventAttrs = event.getAttr();
        Map<String, Map<String, Map<String, SdEventDefinition>>> eventImpls = event.getImpl();

        //构造需要更新的SD
        Sd updateSd = new Sd();
        SdNode updateSDNode = new SdNode();
        SdEvent updateSDEvent = new SdEvent();
        updateSDNode.setAttr(nodeAttrs);
        updateSDEvent.setAttr(eventAttrs);

        HashMap<String, SdNodeDefinition> updateNodeImpls = Maps.newHashMap();
        nodeImpls.forEach((name, impl) -> {
            if (sdNameList.contains(name)) {
                updateNodeImpls.put(name, impl);
            }
        });

        HashMap<String, Map<String, Map<String, SdEventDefinition>>> updateEventImpls = Maps.newHashMap();
        eventImpls.forEach((eventName, leftMap) -> {
            if (!updateEventImpls.containsKey(eventName)) {
                updateEventImpls.put(eventName, Maps.newLinkedHashMap());
            }

            leftMap.forEach((leftNodeName, rightMap) -> {
                if (!updateEventImpls.get(eventName).containsKey(leftNodeName)) {
                    updateEventImpls.get(eventName).put(leftNodeName, Maps.newLinkedHashMap());
                }
                rightMap.forEach((rightNodeName, sdNodeImpl) -> {
                    String eventChain = String.format(
                            "%s%s%s%s%s",
                            leftNodeName,
                            UXDFChain.PATH_LINE,
                            eventName,
                            UXDFChain.PATH_LEFT,
                            rightNodeName
                    );
                    if (sdNameList.contains(eventChain)) {
                        updateEventImpls.get(eventName).get(leftNodeName).put(rightNodeName, sdNodeImpl);
                    }
                });
            });
        });

        updateSDNode.setImpl(updateNodeImpls);
        updateSd.setNode(updateSDNode);
        updateSDEvent.setImpl(updateEventImpls);
        updateSd.setEvent(updateSDEvent);
        return updateSd;
    }
}
