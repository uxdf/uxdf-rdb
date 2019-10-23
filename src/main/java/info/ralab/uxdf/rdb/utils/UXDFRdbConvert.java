package info.ralab.uxdf.rdb.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.UXDFLoader;
import info.ralab.uxdf.definition.SdEventDefinition;
import info.ralab.uxdf.definition.SdNodeDefinition;
import info.ralab.uxdf.definition.SdProperty;
import info.ralab.uxdf.event.UXDFNodeChangeListener;
import info.ralab.uxdf.instance.EventEntity;
import info.ralab.uxdf.instance.NodeEntity;
import info.ralab.uxdf.instance.SdEntity;
import info.ralab.uxdf.model.SdDataQueryParam;
import info.ralab.uxdf.rdb.RdbLoader;
import info.ralab.uxdf.rdb.convert.OracleTimestampConvert;
import info.ralab.uxdf.rdb.convert.BinaryConvert;
import info.ralab.uxdf.rdb.model.*;
import info.ralab.uxdf.utils.AssociateUniquePropertyUtil;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import info.ralab.uxdf.utils.UXDFBinaryFileInfos;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static info.ralab.uxdf.definition.SdBaseType.Binary;

/**
 * UXDF数据和Rdb数据转换类
 */
@Component
@Slf4j
public class UXDFRdbConvert {

    private ApplicationContext applicationContext;
    private RdbLoader rdbLoader;

    @Autowired
    public UXDFRdbConvert(
            final ApplicationContext applicationContext,
            final RdbLoader rdbLoader
    ) {
        this.applicationContext = applicationContext;
        this.rdbLoader = rdbLoader;
    }

    private OracleTimestampConvert oracleTimestampConvert = new OracleTimestampConvert();


    /**
     * Rdb数据集合转换为{@link NodeEntity}集合
     *
     * @param rdbResults Rdb数据集合
     * @return Node实例集合
     */
    public List<NodeEntity> rdbToNodes(final List<JSONObject> rdbResults) {
        List<NodeEntity> nodes = Lists.newArrayListWithCapacity(rdbResults.size());
        for (JSONObject rdbResult : rdbResults) {
            nodes.add(this.rdbToNode(rdbResult));
        }
        return nodes;
    }

    /**
     * Rdb数据集合转换为{@link EventEntity}集合
     *
     * @param rdbResults Rdb数据集合
     * @return Event实例集合
     */
    public List<EventEntity> rdbToEvents(final List<JSONObject> rdbResults) {
        List<EventEntity> events = Lists.newArrayListWithCapacity(rdbResults.size());
        for (JSONObject rdbResult : rdbResults) {
            events.add(this.rdbToEvent(rdbResult));
        }
        return events;
    }

    /**
     * 关系型数据库结果，转换为{@link NodeEntity}。并触发事件通知.
     *
     * @param rdbResult 关系型数据库结果
     * @return Node实例
     */
    public NodeEntity rdbToNode(final JSONObject rdbResult) {
        // 数据库结果为空，直接返回
        if (rdbResult == null) {
            return null;
        }

        // Node未定义返回空
        final String nodeName = rdbResult.getString(RdbAttr.A_SD);
        if (nodeName == null) {
            return null;
        }
        SdNodeDefinition sdNode = UXDFLoader.getNode(nodeName);
        if (sdNode == null) {
            return null;
        }
        // 关系型数据库列映射
        Map<String, String> columnMapping = rdbLoader.getRdbNodeMapping(nodeName).getColumn();
        // Node基本属性定义
        Map<String, SdProperty> nodeAttr = UXDFLoader.getBaseUXDF().getSd().getNode().getAttr();
        // Node扩展属性定义
        Map<String, SdProperty> nodeProp = sdNode.getProp();
        // Node关联唯一属性定义
        Map<String, SdProperty> nodeAssociatedUniqueProp = Maps.newHashMap();
        String nodeAssociatedUniquePropName = AssociateUniquePropertyUtil.getPropertyName(UXDFLoader.getNode(nodeName));
        if (nodeAssociatedUniquePropName != null) {
            nodeAssociatedUniqueProp.put(
                    nodeAssociatedUniquePropName,
                    AssociateUniquePropertyUtil.getProperty(nodeAssociatedUniquePropName)
            );
        }

        NodeEntity nodeEntity = new NodeEntity();
        try {
            // 转换基本属性
            convertColumn(nodeAttr, columnMapping, rdbResult, nodeEntity);
            // 转换扩展属性
            convertColumn(nodeProp, columnMapping, rdbResult, nodeEntity);
            if (nodeAssociatedUniquePropName != null) {
                // 转换关联唯一属性
                convertColumn(nodeAssociatedUniqueProp, columnMapping, rdbResult, nodeEntity);
            }
        } catch (Exception e) {
            throw new UXDFException(e);
        }

        // TODO 权限植入

        // 数据返回事件通知
        UXDFNodeChangeListener nodeChangeListener = UXDFChangeListenerHelper
                .getNodeChangeListener(this.applicationContext, nodeEntity.get__Sd());
        if (nodeChangeListener != null) {
            nodeChangeListener.query(nodeEntity);
        }

        return nodeEntity;
    }

    /**
     * 关系型数据库结果，转换为{@link EventEntity}。并触发事件通知.
     *
     * @param rdbResult 关系型数据库结果
     * @return Event实例
     */
    public EventEntity rdbToEvent(final JSONObject rdbResult) {
        // 数据库结果为空，直接返回
        if (rdbResult == null) {
            return null;
        }

        // Event未定义返回空
        final String eventName = rdbResult.getString(RdbAttr.A_SD);
        final String leftNode = rdbResult.getString(RdbAttr.A_LEFT_SD);
        final String rightNode = rdbResult.getString(RdbAttr.A_RIGHT_SD);
        SdEventDefinition sdEvent = UXDFLoader.getEvent(eventName, leftNode, rightNode);
        if (sdEvent == null) {
            return null;
        }

        // 关系型数据库列映射
        Map<String, String> columnMapping = rdbLoader.getRdbEventMapping(eventName).get(leftNode).get(rightNode).getColumn();
        // Event基本属性定义
        Map<String, SdProperty> eventAttr = UXDFLoader.getBaseUXDF().getSd().getEvent().getAttr();
        // Event扩展属性定义
        Map<String, SdProperty> eventProp = sdEvent.getProp();

        EventEntity eventEntity = new EventEntity();
        try {
            // 转换基本属性
            convertColumn(eventAttr, columnMapping, rdbResult, eventEntity);

            // 转换扩展属性
            convertColumn(eventProp, columnMapping, rdbResult, eventEntity);
        } catch (Exception e) {
            throw new UXDFException(e);
        }

        // 权限植入

        return eventEntity;
    }

    /**
     * 转换列数据到{@link SdEntity}中
     *
     * @param propertyDefinitions Sd属性定义
     * @param columnMapping       Sd属性和列映射
     * @param rdbResult           数据库结果
     * @param sdEntity            Sd实例
     * @throws SQLException SQL异常
     * @throws IOException  输入输入异常
     */
    private void convertColumn(
            final Map<String, SdProperty> propertyDefinitions,
            final Map<String, String> columnMapping,
            final JSONObject rdbResult,
            final SdEntity sdEntity
    ) throws SQLException, IOException {
        // 遍历属性定义
        for (String propertyName : propertyDefinitions.keySet()) {
            // 获取对应列名
            final String column = columnMapping.get(propertyName);
            // 如果Rdb结果未包含列，跳过
            if (!rdbResult.containsKey(column)) {
                continue;
            }
            // 获取属性定义
            SdProperty prop = propertyDefinitions.get(propertyName);
            // 获取取值
            Object value = rdbResult.get(column);
            // 进行类型转换
            switch (prop.getBase()) {
                // 二进制
                case Binary:
                    break;
                // 浮点
                case Float:
                    break;
                // 字符
                case String:
                    // 是否大数据
                    if (value instanceof Clob) {
                        Clob clob = (Clob) value;
                        // 读取字符流
                        try (Reader reader = clob.getCharacterStream()) {
                            StringBuilder builder = new StringBuilder();
                            char[] chars = new char[1024];
                            int readLength;
                            while ((readLength = reader.read(chars)) > -1) {
                                builder.append(chars, 0, readLength);
                            }
                            value = builder.toString();
                        }
                    } else {
                        value = rdbResult.getString(column);
                    }
                    break;
                // 布尔值
                case Boolean:
                    value = rdbResult.getBoolean(column);
                    break;
                // 整数
                case Integer:
                    break;
                // 时间
                case Datetime:
                    break;
            }

            sdEntity.put(propertyName, value);
        }
    }

    /**
     * 获取{@link NodeEntity}的RDB新增参数{@link InsertParam}
     *
     * @param node Node实例
     * @return 新增参数
     */
    public InsertParam makeNodeInsertParam(final NodeEntity node) {
        // 获取关系型数据库映射关系
        final String nodeName = node.get__Sd();
        RelationShipRdbTable nodeMapping = rdbLoader.getRdbNodeMapping(nodeName);
        // 获取属性定义
        Map<String, SdProperty> nodeAttr = UXDFLoader.getBaseUXDF().getSd().getNode().getAttr();
        Map<String, SdProperty> nodeProp = UXDFLoader.getNode(nodeName).getProp();
        // 表名
        String tableName = nodeMapping.getName();
        // 序列
        String seqId = nodeMapping.getSeqId();
        // 字段映射
        Map<String, String> columnMapping = nodeMapping.getColumn();

        // 构建新增参数
        InsertParam insertParam = new InsertParam();
        insertParam.setSeqId(seqId);
        insertParam.setId(node.get__Id());
        insertParam.setTable(tableName);
        insertParam.setSd(nodeName);
        // 遍历Node实例的属性
        node.keySet().forEach((propertyName) -> {
            // 跳过ID
            if (NodeEntity.ATTR_ID.equals(propertyName)) {
                return;
            }
            // 处理有映射关系的属性
            if (columnMapping.containsKey(propertyName)) {
                // 加入新增列集合
                insertParam.getColumns().add(columnMapping.get(propertyName));
                // 获取属性定义
                SdProperty property = nodeProp.containsKey(propertyName) ?
                        nodeProp.get(propertyName) : nodeAttr.get(propertyName);
                // 如果属性不在默认和扩展中，尝试通过关联唯一获取
                if (property == null) {
                    property = AssociateUniquePropertyUtil.getProperty(propertyName);
                }
                // 属性定义未找到
                if (property == null) {
                    throw new UXDFException(String.format("未找到属性[%s]定义。", propertyName));
                }
                // 获取属性值
                Object value = node.get(propertyName);
                // 属性值为空时，尝试使用默认值填充
                if (value == null) {
                    value = property.getDefaultValue();
                }
                // 转换属性值
                value = convertValue(node, property, value);
                // 二进制需要额外处理
                if (property.getBase() == Binary && value instanceof UXDFBinaryFileInfo) {
                    try {
                        value = UXDFBinaryFileInfos.convertToInputStream((UXDFBinaryFileInfo) value);
                    } catch (IOException e) {
                        throw new UXDFException(e);
                    }
                }
                // 加入新增值集合
                insertParam.getValues().add(value);
            }
        });

        return insertParam;
    }

    /**
     * 获取{@link NodeEntity}的RDB更新参数{@link UpdateParam}
     *
     * @param node Node实例
     * @return 更新参数
     */
    public UpdateParam makeNodeUpdateParam(final NodeEntity node) {
        // 获取关系型数据库映射关系
        final String nodeName = node.get__Sd();
        RelationShipRdbTable nodeMapping = rdbLoader.getRdbNodeMapping(nodeName);
        // 获取属性定义
        Map<String, SdProperty> nodeAttr = UXDFLoader.getBaseUXDF().getSd().getNode().getAttr();
        Map<String, SdProperty> nodeProp = UXDFLoader.getNode(nodeName).getProp();
        // 表名
        String tableName = nodeMapping.getName();
        // 字段映射
        Map<String, String> columnMapping = nodeMapping.getColumn();

        UpdateParam updateParam = new UpdateParam(
                node.get__Id(),
                tableName
        );
        // 设置同步锁
        if (node.get(SdEntity.DYNA_SYNC_LOCK) instanceof SyncLock) {
            SyncLock syncLock = (SyncLock) node.get(SdEntity.DYNA_SYNC_LOCK);
            // 替换属性为列
            if (columnMapping.containsKey(syncLock.getColumn())) {
                syncLock.setColumn(columnMapping.get(syncLock.getColumn()));
                updateParam.setSyncLock(syncLock);
            }
        }

        // 遍历属性
        node.keySet().forEach((propertyName) -> {
            // 处理在列映射中的属性
            if (columnMapping.containsKey(propertyName)) {
                // 获取属性定义
                SdProperty property = nodeProp.containsKey(propertyName) ?
                        nodeProp.get(propertyName) : nodeAttr.get(propertyName);
                // 属性定义不是默认属性也不是扩展属性，尝试获取关联唯一属性
                if (property == null) {
                    property = AssociateUniquePropertyUtil.getProperty(propertyName);
                }
                // 获取列名称
                final String column = columnMapping.get(propertyName);
                // 获取属性取值
                Object value = node.get(propertyName);
                // 属性取值为空，尝试用默认值
                if (value == null) {
                    value = property.getDefaultValue();
                }
                // 转换取值
                value = convertValue(node, property, value);
                // 二进制需要额外处理
                if (property.getBase() == Binary) {
                    if (value instanceof UXDFBinaryFileInfo) {
                        try {
                            value = UXDFBinaryFileInfos.convertToInputStream((UXDFBinaryFileInfo) value);
                        } catch (IOException e) {
                            throw new UXDFException(e);
                        }
                        // 将字段和取值添加到更新参数
                        updateParam.getColumnValues().add(new UpdateColumnValue(column, value));
                    }
                    // TODO 二进制暂时不支持更新删除
                } else {
                    // 将字段和取值添加到更新参数
                    updateParam.getColumnValues().add(new UpdateColumnValue(column, value));
                }
            }
        });

        return updateParam;
    }

    /**
     * 获取{@link EventEntity}的RDB新增参数{@link InsertParam}
     *
     * @param event Event实例
     * @return 新增参数
     */
    public InsertParam makeEventInsertParam(final EventEntity event) {
        final String eventName = event.get__Sd();
        final String leftNode = event.get__LeftSd();
        final String rightNode = event.get__RightSd();
        RelationShipRdbTable eventMapping = rdbLoader.getRdbEventMapping(eventName).get(leftNode).get(rightNode);
        Map<String, SdProperty> eventAttr = UXDFLoader.getBaseUXDF().getSd().getEvent().getAttr();
        Map<String, SdProperty> eventProp = UXDFLoader.getEvent(eventName).get(leftNode).get(rightNode).getProp();
        String tableName = eventMapping.getName();
        String seqId = eventMapping.getSeqId();
        Map<String, String> columnMapping = eventMapping.getColumn();

        InsertParam insertParam = new InsertParam();
        insertParam.setSeqId(seqId);
        insertParam.setId(event.get__Id());
        insertParam.setTable(tableName);
        insertParam.setSd(eventName);

        insertParam.setLeft(event.get__Left());
        insertParam.setLeftSd(event.get__LeftSd());

        insertParam.setRight(event.get__Right());
        insertParam.setRightSd(event.get__RightSd());
        event.keySet().forEach((key) -> {
            if (EventEntity.ATTR_ID.equals(key)) {
                return;
            }
            if (columnMapping.containsKey(key)) {
                insertParam.getColumns().add(columnMapping.get(key));
                SdProperty prop = eventProp.containsKey(key) ? eventProp.get(key) : eventAttr.get(key);
                Object value = event.get(key);
                if (value == null) {
                    value = prop.getDefaultValue();
                }
                value = convertValue(event, prop, value);

                insertParam.getValues().add(value);
            }
        });

        return insertParam;
    }

    /**
     * 获取{@link EventEntity}的RDB更新参数{@link UpdateParam}
     *
     * @param event Event实例
     * @return 更新参数
     */
    public UpdateParam makeEventUpdateParam(final EventEntity event) {
        final String eventName = event.get__Sd();
        final String leftNode = event.get__LeftSd();
        final String rightNode = event.get__RightSd();
        RelationShipRdbTable eventMapping = rdbLoader.getRdbEventMapping(eventName).get(leftNode).get(rightNode);
        Map<String, SdProperty> eventAttr = UXDFLoader.getBaseUXDF().getSd().getEvent().getAttr();
        Map<String, SdProperty> eventProp = UXDFLoader.getEvent(eventName).get(leftNode).get(rightNode).getProp();
        String tableName = eventMapping.getName();
        Map<String, String> columnMapping = eventMapping.getColumn();

        UpdateParam updateParam = new UpdateParam(
                event.get__Id(),
                tableName
        );
        event.keySet().forEach((key) -> {
            if (columnMapping.containsKey(key)) {
                SdProperty prop = eventProp.containsKey(key) ? eventProp.get(key) : eventAttr.get(key);
                final String column = columnMapping.get(key);
                Object value = event.get(key);
                if (value == null) {
                    value = prop.getDefaultValue();
                }
                value = convertValue(event, prop, value);

                updateParam.getColumnValues().add(new UpdateColumnValue(column, value));
            }
        });

        return updateParam;
    }

    /**
     * 生成{@link NodeEntity}查询参数{@link SdDataQueryParam}集合
     *
     * @param nodeName    Node定义名称
     * @param queryParams 查询参数集合
     * @return 转换后的查询参数集合
     */
    public List<SdDataQueryParam> convertNodeQueryParam(
            final String nodeName,
            final List<SdDataQueryParam> queryParams
    ) {
        if (queryParams == null || queryParams.isEmpty()) {
            return Lists.newArrayList();
        }

        RelationShipRdbTable nodeMapping = rdbLoader.getRdbNodeMapping(nodeName);
        Map<String, SdProperty> nodeAttr = UXDFLoader.getBaseUXDF().getSd().getNode().getAttr();
        Map<String, SdProperty> nodeProp = UXDFLoader.getNode(nodeName).getProp();
        Map<String, String> columnMapping = nodeMapping.getColumn();

        List<SdDataQueryParam> params = Lists.newArrayList();

        queryParams.forEach(queryParam -> {
            String propertyName = queryParam.getProperty();
            if (columnMapping.containsKey(propertyName)) {
                SdProperty prop = nodeProp.containsKey(propertyName) ?
                        nodeProp.get(propertyName) : nodeAttr.get(propertyName);
                // 属性定义不是默认属性也不是扩展属性，尝试获取关联唯一属性
                if (prop == null) {
                    prop = AssociateUniquePropertyUtil.getProperty(propertyName);
                }
                // 二进制不可以被查询
                if (prop.getBase() == Binary) {
                    return;
                }

                Object queryValue = queryParam.getValue();
                // 此处无法传入sdEntity，sdEntity只在获取binary类型使用。binary不可以被查询，所以可以传入null。
                queryValue = convertValue(null, prop, queryValue);

                params.add(new SdDataQueryParam(
                        columnMapping.get(propertyName),
                        queryValue,
                        queryParam.getLogic()
                ));
            }
        });

        return params;
    }

    /**
     * 生成{@link EventEntity}查询参数{@link SdDataQueryParam}集合
     *
     * @param eventName     Event定义名称
     * @param leftNodeName  Event左Node定义名称
     * @param rightNodeName Event右Node定义名称
     * @param queryParams   查询参数集合
     * @return 转换后的查询参数集合
     */
    public List<SdDataQueryParam> convertEventQueryParam(
            String eventName,
            String leftNodeName,
            String rightNodeName,
            List<SdDataQueryParam> queryParams
    ) {
        if (queryParams == null || queryParams.isEmpty()) {
            return Lists.newArrayList();
        }

        RelationShipRdbTable eventMapping = rdbLoader.getRdbEventMapping(eventName).get(leftNodeName).get(rightNodeName);
        Map<String, SdProperty> eventAttr = UXDFLoader.getBaseUXDF().getSd().getEvent().getAttr();
        Map<String, SdProperty> eventProp = UXDFLoader.getEvent(eventName).get(leftNodeName).get(rightNodeName).getProp();
        Map<String, String> columnMapping = eventMapping.getColumn();

        List<SdDataQueryParam> params = Lists.newArrayList();

        queryParams.forEach(queryParam -> {
            String property = queryParam.getProperty();
            if (columnMapping.containsKey(property)) {
                SdProperty prop = eventProp.containsKey(property) ?
                        eventProp.get(property) : eventAttr.get(property);
                Object queryValue = queryParam.getValue();
                // 此处无法传入sdEntity，sdEntity只在获取binary类型使用。binary不可以被查询，所以可以传入null。
                queryValue = convertValue(null, prop, queryValue);

                params.add(new SdDataQueryParam(
                        columnMapping.get(property),
                        queryValue,
                        queryParam.getLogic()
                ));
            }
        });

        return params;
    }

    /**
     * 根据属性基本类型，转换数据
     *
     * @param sdEntity Sd实例
     * @param property 属性定义
     * @param value    属性值
     * @return 转换后的属性值
     */
    private Object convertValue(SdEntity sdEntity, SdProperty property, Object value) {
        if (value instanceof List) {
            List<Object> values = Lists.newArrayList();

            ((List<Object>) value).forEach(valueItem -> values.add(convertValue(sdEntity, property, valueItem)));

            value = values;
        } else {
            switch (property.getBase()) {
                case Integer:
                    value = SdEntity.getBaseInteger(value);
                    break;
                case String:
                    value = SdEntity.getBaseString(value);
                    break;
                case Float:
                    value = SdEntity.getBaseFloat(value);
                    break;
                case Boolean:
                    value = SdEntity.getBaseBoolean(value);
                    break;
                case Datetime:
                    value = SdEntity.getBaseDate(value, oracleTimestampConvert);
                    break;
                case Binary:
                    value = SdEntity.getBaseBinary(value, sdEntity, new BinaryConvert(sdEntity));
                    break;
            }
        }
        return value;
    }

}
