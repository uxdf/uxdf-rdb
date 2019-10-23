package info.ralab.uxdf.rdb;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.SdData;
import info.ralab.uxdf.UXDF;
import info.ralab.uxdf.UXDFLoader;
import info.ralab.uxdf.chain.UXDFChain;
import info.ralab.uxdf.definition.*;
import info.ralab.uxdf.instance.EventEntity;
import info.ralab.uxdf.instance.IdMaker;
import info.ralab.uxdf.instance.NodeEntity;
import info.ralab.uxdf.model.SdDataQueryParam;
import info.ralab.uxdf.model.SdDataQueryRequest;
import info.ralab.uxdf.model.SdDataQueryResult;
import info.ralab.uxdf.model.SdDataSaveResult;
import info.ralab.uxdf.rdb.exception.UXDFSaveErrorType;
import info.ralab.uxdf.rdb.exception.UXDFSaveException;
import info.ralab.uxdf.rdb.executor.SaveExecutor;
import info.ralab.uxdf.rdb.mapper.UXDFMapper;
import info.ralab.uxdf.rdb.mapper.UXDFQueryMapper;
import info.ralab.uxdf.rdb.executor.QueryRequestExecutor;
import info.ralab.uxdf.rdb.model.InsertParam;
import info.ralab.uxdf.rdb.model.RelationShipRdbTable;
import info.ralab.uxdf.rdb.model.UpdateParam;
import info.ralab.uxdf.rdb.utils.UXDFRdbConvert;
import info.ralab.uxdf.rdb.utils.UXDFRdbValidator;
import info.ralab.uxdf.service.StorageService;
import info.ralab.uxdf.utils.AssociateUniquePropertyUtil;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.MessageSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.io.InputStream;
import java.util.*;

import static org.springframework.transaction.annotation.Propagation.MANDATORY;

/**
 * 关系型数据库存储服务
 */
@Component("uxdfRdbStorageService")
@Slf4j
public class RdbStorageService implements StorageService {

    private ApplicationContext applicationContext;
    private RdbLoader rdbLoader;
    private UXDFRdbConvert uxdfRdbConvert;
    private UXDFQueryMapper uxdfQueryMapper;
    private UXDFMapper uxdfMapper;
    private MessageSource messageSource;
    private JdbcTemplate jdbcTemplate;


    private UXDFRdbValidator rdbValidator;

    @Autowired
    public RdbStorageService(
            final ApplicationContext applicationContext,
            final RdbLoader rdbLoader,
            final UXDFRdbConvert uxdfRdbConvert,
            final UXDFQueryMapper uxdfQueryMapper,
            final UXDFMapper uxdfMapper,
            final MessageSource messageSource,
            final JdbcTemplate jdbcTemplate
    ) {
        this.applicationContext = applicationContext;
        this.rdbLoader = rdbLoader;
        this.uxdfRdbConvert = uxdfRdbConvert;
        this.uxdfQueryMapper = uxdfQueryMapper;
        this.uxdfMapper = uxdfMapper;
        this.messageSource = messageSource;
        this.jdbcTemplate = jdbcTemplate;

        this.rdbValidator = new UXDFRdbValidator(this, this.messageSource);
    }

    /**
     * 初始服务
     */
    @Override
    public void init() {
        rdbLoader.load(true);
    }

    /**
     * 销毁服务
     */
    @Override
    public void destory() {

    }

    /**
     * @param sdData  需要保存的Sd数据
     * @param operate 默认操作类型。如果Sd实例中未指定操作类型，并且默认操作类型不为null，那么将按照默认操作类型处理
     * @param files   保存Sd实例中的二进制文件集合，下表和具体Sd实例属性值一致
     * @param sync    是否对保存完的数据，进行和存储中的同步
     * @return 保存结果
     */
    @Transactional(propagation = MANDATORY)
    @Override
    public SdDataSaveResult saveData(
            final SdData sdData,
            final SdOperateType operate,
            final UXDFBinaryFileInfo[] files,
            final boolean sync
    ) {
        return new SaveExecutor(
                applicationContext,
                this,
                this.messageSource,
                sdData
        )
                .files(files)
                .operate(operate)
                .execute();
    }

    @Override
    public SdDataSaveResult saveData(SdData sdData) {
        return this.saveData(sdData, null, null, false);
    }

    @Override
    public SdDataSaveResult saveAndSyncData(SdData sdData) {
        return this.saveData(sdData, null, null, true);
    }

    @Override
    public NodeEntity saveNode(NodeEntity nodeEntity) {
        SdData sdData = new SdData();
        sdData.addNodeIfAbsent(nodeEntity);

        this.saveData(sdData);

        if (sdData.isEmpty()) {
            return null;
        } else {
            return sdData.getUnmodifiableNode().get(0);
        }
    }

    @Override
    public EventEntity saveEvent(EventEntity eventEntity) {
        SdData sdData = new SdData();
        sdData.addEventIfAbsent(eventEntity);

        this.saveData(sdData);

        if (sdData.isEmpty()) {
            return null;
        } else {
            return sdData.getUnmodifiableEvent(eventEntity.get__Sd()).get(0);
        }
    }

    /**
     * 使用{@link SdDataQueryRequest}查询{@link UXDF}。
     *
     * @param queryRequest 查询请求
     * @return 符合查询请求的UXDF
     */
    @Override
    public SdDataQueryResult queryData(final SdDataQueryRequest queryRequest) {
        return makeQueryRequestExecutor(queryRequest).execute();
    }

    /**
     * 使用Node定义、逻辑ID、版本库、版本库分支、版本库版本获取唯一的{@link info.ralab.uxdf.instance.NodeEntity}。
     *
     * @param nodeName Node定义
     * @param nodeId   逻辑ID
     * @return 查询结果
     */
    @Override
    public SdDataQueryResult getDataById(
            final String nodeName,
            final String nodeId
    ) {
        // 构造查询请求
        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
        queryRequest.getChains().add(nodeName);
        queryRequest.getParams().put(nodeName, Lists.newArrayList(
                SdDataQueryParam.equal(NodeEntity.ATTR_ID, nodeId)
        ));

        return makeQueryRequestExecutor(queryRequest).execute();
    }

    @Override
    public SdDataQueryResult getDataById(String nodeName, String nodeId, boolean isBlock) {
        return null;
    }

    @Override
    public NodeEntity getNodeEntityById(String nodeName, String nodeId) {
        List<NodeEntity> list = getDataById(nodeName, nodeId)
                .getUxdf()
                .getData()
                .getUnmodifiableNode();

        if (list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    @Override
    public NodeEntity getNodeEntityById(String nodeName, String nodeId, boolean isBlock) {
        return null;
    }

    @Override
    public NodeEntity getNodeEntityByUUID(String nodeName, String uuid) {
        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
        queryRequest.getChains().add(nodeName);
        queryRequest.getParams().put(nodeName, Lists.newArrayList(
                SdDataQueryParam.equal(NodeEntity.ATTR_UUID, uuid)
        ));
        SdDataQueryResult queryResult = makeQueryRequestExecutor(queryRequest).execute();
        List<NodeEntity> list = queryResult.getUxdf().getData().getUnmodifiableNode();

        if (list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    @Override
    public NodeEntity getNodeEntityByUUID(String nodeName, String uuid, boolean isBlock) {
        return null;
    }

    @Override
    public SdDataQueryResult getData(
            String eventName,
            String eventId,
            String leftNodeName,
            String leftNodeId,
            String rightNodeName,
            String rightNodeId
    ) {
        return null;
    }

    @Override
    public SdDataQueryResult getData(
            String eventName,
            String eventId,
            String leftNodeName,
            String leftNodeId,
            String rightNodeName,
            String rightNodeId,
            boolean isBlock
    ) {
        return null;
    }

    @Override
    public EventEntity getEventEntity(
            String eventName,
            String eventId,
            String leftNodeName,
            String leftNodeId,
            String rightNodeName,
            String rightNodeId
    ) {
        return null;
    }

    @Override
    public EventEntity getEventEntity(
            String eventName,
            String eventId,
            String leftNodeName,
            String leftNodeId,
            String rightNodeName,
            String rightNodeId,
            boolean isBlock
    ) {
        return null;
    }

    @Override
    public EventEntity getEventEntity(String eventName, String uuid) {
        return null;
    }

    @Override
    public EventEntity getEventEntity(String eventName, String uuid, boolean isBlock) {
        return null;
    }

    /**
     * 获取二进制文件流{@link InputStream}
     *
     * @param nodeName Node名称
     * @param uuid     唯一标识
     * @return 二进制文件
     */
    @Override
    public InputStream getUXDFBinaryFile(final String nodeName, final String property, final String uuid) {
        RelationShipRdbTable rdbTable = this.rdbLoader.getRdbNodeMapping(nodeName);
        // 表或字段不存在
        if (rdbTable == null || !rdbTable.getColumn().containsKey(property)) {
            return null;
        }
        String table = rdbTable.getName();
        String column = rdbTable.getColumn().get(property);

        return this.uxdfMapper.getBinary(table, column, uuid);
    }

    /**
     * 原始查询
     *
     * @param expression 原始查询表达式
     * @return 查询结果集合
     */
    @Override
    public List<Map<String, Object>> originalQuery(final String expression) {
        return this.jdbcTemplate.queryForList(expression);
    }

    /**
     * 解除两个NodeEntity之间的指定类型的Event
     *
     * @param leftNode  左节点
     * @param rightNode 右节点
     * @param eventType 事件类型
     * @return 实际移除的Event数量
     */
    @Override
    public int unlink(final NodeEntity leftNode, final NodeEntity rightNode, final String eventType) {
        RelationShipRdbTable rdbTable = this.rdbLoader.getRdbEventMapping(
                eventType, leftNode.get__Sd(), rightNode.get__Sd()
        );

        return this.uxdfMapper.deleteEventByLeftAndRight(
                rdbTable.getName(),
                leftNode.get__Id(),
                rightNode.get__Id()
        );
    }

    /**
     * 完全清除指定Node的全部数据
     *
     * @param nodeName 节点名称
     * @return 清除数量
     */
    @Override
    public int clearNode(final String nodeName) {
        RelationShipRdbTable rdbTable = this.rdbLoader.getRdbNodeMapping(nodeName);
        return this.uxdfMapper.deleteNodeAll(rdbTable.getName());
    }


    /**
     * 使用Node实体属性获取一个完整Node实体。
     *
     * @param nodeEntity   Node实体，包含作为查询条件的属性
     * @param useUniqueKey 是否只使用唯一属性查询
     * @return Node实例
     */
    public NodeEntity loadNodeByEntity(final NodeEntity nodeEntity, final boolean useUniqueKey) {
        // 数据不完整
        if (nodeEntity == null || !nodeEntity.isEffective()) {
            return null;
        }
        // 获取SD定义
        SdNodeDefinition sdNode = UXDFLoader.getNode(nodeEntity.get__Sd());
        // 定义不存在
        if (sdNode == null) {
            return null;
        }

        List<SdDataQueryParam> queryParams = Lists.newArrayList();

        if (useUniqueKey) { // 只使用唯一属性作为查询条件
            String[] uniqueIndex = sdNode.getUniqueIndex();
            if (uniqueIndex == null || uniqueIndex.length < 1) {
                return null;
            }
            for (String propertyName : uniqueIndex) {
                Object value = nodeEntity.get(propertyName);
                // 唯一属性缺失
                if (value == null) {
                    if (UXDFChain.haveRelationship(propertyName)) {
                        // 唯一属性有关系表达式，获取关联唯一属性值
                        final String uniqueProperty = AssociateUniquePropertyUtil.getPropertyName(sdNode);
                        // 无法获取属性名
                        if (uniqueProperty == null) {
                            return null;
                        }
                        value = nodeEntity.get(uniqueProperty);
                        // 关联唯一属性无法获取
                        if (value == null) {
                            return null;
                        }
                        queryParams.add(SdDataQueryParam.equal(uniqueProperty, value));
                    } else {
                        return null;
                    }
                } else {
                    queryParams.add(SdDataQueryParam.equal(propertyName, value));
                }
            }
        } else { // 使用全部已有属性作为查询条件（包括Attr，关联唯一属性）
            Map<String, SdProperty> propList = Maps.newHashMap(sdNode.getProp());
            propList.putAll(UXDFLoader.getBaseUXDF().getSd().getNode().getAttr());
            // Node关联唯一属性定义
            String nodeAssociatedUniquePropName = AssociateUniquePropertyUtil.getPropertyName(UXDFLoader.getNode(nodeEntity.get__Sd()));
            if (nodeAssociatedUniquePropName != null) {
                propList.put(
                        nodeAssociatedUniquePropName,
                        AssociateUniquePropertyUtil.getProperty(nodeAssociatedUniquePropName)
                );
            }
            propList.forEach((propertyName, sdProp) -> {
                // 加入所有非空属性
                Object value = nodeEntity.get(propertyName);
                if (value != null) {
                    queryParams.add(SdDataQueryParam.equal(propertyName, value));
                }
            });
        }

        // 构建查询请求
        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
        queryRequest.getChains().add(sdNode.getNodeName());
        queryRequest.getParams().put(sdNode.getNodeName(), queryParams);

        SdDataQueryResult queryResult = makeQueryRequestExecutor(queryRequest).execute();
        // 未找到数据
        if (queryResult.getUxdf().getData().isNodeEmpty()) {
            return null;
        }

        // 返回首条数据
        return queryResult.getUxdf().getData().getUnmodifiableNode().get(0);
    }

    /**
     * 创建查询请求执行类
     *
     * @param queryRequest 查询请求
     * @return 查询请求执行类
     */
    public QueryRequestExecutor makeQueryRequestExecutor(final SdDataQueryRequest queryRequest) {
        return new QueryRequestExecutor(
                this.rdbLoader,
                this.uxdfRdbConvert,
                this.uxdfQueryMapper,
                queryRequest
        );
    }

    /**
     * 新增Node
     *
     * @param node Node实例
     * @param data 数据集合
     * @return 新增实例数量
     */
    public int createNode(
            final NodeEntity node,
            final SdData data
    ) {
        // 获取ID
        String nodeId = node.isCreateOriginalId() ? node.get__Id() : IdMaker.next();
        // 更新相关Event的Id
        updateEventId(data, node, nodeId);
        node.set__Id(nodeId);
        // 重新生成UUID
        node.generateUUID();
        // 统一设置创建时间
        node.set__CreateTime(new Date());
        node.set__UpdateTime(node.get__CreateTime());

        // TODO 数据新增前事件通知


        // 检查数据
        rdbValidator.check(node);

        // 获取Insert参数
        InsertParam insertParam = this.uxdfRdbConvert.makeNodeInsertParam(node);
        log.debug("insertParam:{}", insertParam);
        final int insertNum = this.uxdfMapper.insert(insertParam);
        node.set__Id(insertParam.getId());

        // TODO 保存后数据处理

        // 移除动态属性
        node.removeDynamicAttr();

        return insertNum;
    }

    /**
     * 更新Node实例{@link NodeEntity}
     *
     * @param node 被更新的Node实例
     */
    public int updateNode(
            final NodeEntity node,
            final SdData sdData
    ) {
        // 重新生成UUID
        node.generateUUID();
        // 统一设置更新时间
        node.set__UpdateTime(new Date());

        // TODO 更新前通知

        // 检查数据
        rdbValidator.check(node);

        // 获取update参数
        UpdateParam updateParam = this.uxdfRdbConvert.makeNodeUpdateParam(node);

        final int updateNum = this.uxdfMapper.update(updateParam);

        // TODO 更新后数据处理

        node.removeDynamicAttr();

        return updateNum;
    }

    /**
     * 删除Node实例{@link NodeEntity}
     *
     * @param node Node实例{
     */
    public int deleteNode(
            final NodeEntity node,
            final SdData sdData
    ) {
        // 重新生成UUID
        node.generateUUID();
        final String nodeName = node.get__Sd();
        final String nodeId = node.get__Id();
        final boolean enforce = node.isDeleteEnforce();

        // 检查数据
        rdbValidator.check(node);

        int deletedNum = 0;

        final RelationShipRdbTable nodeMapping = rdbLoader.getRdbNodeMapping(nodeName);
        final String nodeTable = nodeMapping.getName();

        Set<SdEventDefinition> eventDefinitions = UXDFLoader.getEventsByNodeName(nodeName);
        if (eventDefinitions != null) {
            for (SdEventDefinition eventDefinition : eventDefinitions) {
                final String eventName = eventDefinition.getEventName();
                final String leftNodeName = eventDefinition.getLeftNodeName();
                final String rightNodeName = eventDefinition.getRightNodeName();

                final RelationShipRdbTable eventMapping = rdbLoader.getRdbEventMapping(eventName)
                        .get(leftNodeName)
                        .get(rightNodeName);
                final String eventTable = eventMapping.getName();

                final RelationShipRdbTable leftNodeMapping = rdbLoader.getRdbNodeMapping(leftNodeName);
                final String leftNodeTable = leftNodeMapping.getName();

                final RelationShipRdbTable rightNodeMapping = rdbLoader.getRdbNodeMapping(rightNodeName);
                final String rightNodeTable = rightNodeMapping.getName();

                // 当前Node是左节点
                if (leftNodeName.equals(nodeName)) {
                    // 该Event对于右节点不是必须
                    if (eventDefinition.getRequired() == null
                            || eventDefinition.getRequired() == SdEventRequiredType.none
                            ||
                            (eventDefinition.getRequired() != SdEventRequiredType.right
                                    && eventDefinition.getRequired() != SdEventRequiredType.both)
                    ) {
                        deletedNum += this.uxdfMapper.deleteEventByLeftNode(
                                eventTable,
                                nodeId,
                                nodeName
                        );
                    } else if (enforce) {
                        // 该Event对于右节点是必须，并且可以强制删除
                        // 获取相关Node，并进行逐一删除
                        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
                        String targetLabel = "TN";
                        queryRequest.getChains().add(String.format(
                                "%s-%s>%s:%s",
                                leftNodeName,
                                eventName,
                                targetLabel,
                                rightNodeName
                        ));
                        queryRequest.getReturns().add(targetLabel);
                        // 通过左Node查找
                        queryRequest.getParams().put(leftNodeName, Lists.newArrayList(
                                SdDataQueryParam.equal(NodeEntity.ATTR_SD, nodeName),
                                SdDataQueryParam.equal(NodeEntity.ATTR_ID, nodeId)
                        ));
                        SdDataQueryResult queryResult = this.makeQueryRequestExecutor(queryRequest)
                                .execute();
                        for (NodeEntity rightNode : queryResult.getUxdf().getData().getUnmodifiableNode()) {
                            rightNode.setOperateDeleteEnforce(Boolean.TRUE);
                            deletedNum += this.deleteNode(rightNode, sdData);
                        }
                        // 后删除Event
                        deletedNum += this.uxdfMapper.deleteEventByLeftNode(
                                eventTable,
                                nodeId,
                                nodeName
                        );
                    } else { // 该Event对于右节点是必须，并且不可以强制删除
                        throw new UXDFSaveException(
                                messageSource.getMessage(
                                        "error.save.cascade.node",
                                        new Object[]{leftNodeName, nodeId, eventName, rightNodeName},
                                        Locale.getDefault()
                                ),
                                SdOperateType.delete,
                                UXDFSaveErrorType.cascade
                        );
                    }
                }

                // 当前Node是右节点
                if (rightNodeName.equals(nodeName)) {
                    // 该Event对于左节点不是必须
                    if (eventDefinition.getRequired() == null
                            || eventDefinition.getRequired() == SdEventRequiredType.none
                            ||
                            (eventDefinition.getRequired() != SdEventRequiredType.left
                                    && eventDefinition.getRequired() != SdEventRequiredType.both)
                    ) {
                        deletedNum += this.uxdfMapper.deleteEventByRightNode(
                                eventTable,
                                nodeId,
                                nodeName
                        );
                    } else if (enforce) {
                        // 该Event对于左节点是必须，并且可以强制删除
                        // 获取相关Node，并进行逐一删除
                        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
                        String targetLabel = "TN";
                        queryRequest.getChains().add(String.format(
                                "%s<%s-%s:%s",
                                rightNodeName,
                                eventName,
                                targetLabel,
                                leftNodeName
                        ));
                        queryRequest.getReturns().add(targetLabel);
                        // 通过右Node查找
                        queryRequest.getParams().put(rightNodeName, Lists.newArrayList(
                                SdDataQueryParam.equal(NodeEntity.ATTR_SD, nodeName),
                                SdDataQueryParam.equal(NodeEntity.ATTR_ID, nodeId)
                        ));
                        SdDataQueryResult queryResult = this.makeQueryRequestExecutor(queryRequest)
                                .execute();
                        for (NodeEntity leftNode : queryResult.getUxdf().getData().getUnmodifiableNode()) {
                            leftNode.setOperateDeleteEnforce(Boolean.TRUE);
                            deletedNum += this.deleteNode(leftNode, sdData);
                        }
                        // 后删除Event
                        deletedNum += this.uxdfMapper.deleteEventByRightNode(
                                eventTable,
                                nodeId,
                                nodeName
                        );
                    } else { // 该Event对于右节点是必须，并且不可以强制删除
                        throw new UXDFSaveException(
                                messageSource.getMessage(
                                        "error.save.cascade.node",
                                        new Object[]{rightNodeName, nodeId, eventName, leftNodeName},
                                        Locale.getDefault()
                                ),
                                SdOperateType.delete,
                                UXDFSaveErrorType.cascade
                        );
                    }
                }
            }
        }

        // 最后删除当前Node
        deletedNum += this.uxdfMapper.deleteNode(
                nodeTable,
                nodeId,
                null
        );
        // 重新返回，供前端同步数据
        node.setOperate(SdOperateType.delete);
        sdData.addNodeIfAbsent(node);

        // TODO 处理需要包装的数据 删除后数据处理

        return deletedNum;
    }

    /**
     * 创建Event实例{@link EventEntity}
     *
     * @param event Event实例
     * @return 成功创建数量
     */
    public int createEvent(
            final EventEntity event,
            final SdData sdData
    ) {
        // 重新生成UUID
        event.generateUUID();
        // 获取ID
        event.set__Id(IdMaker.next());
        // 统一设置创建时间
        event.set__CreateTime(new Date());
        event.set__UpdateTime(event.get__CreateTime());

        this.rdbValidator.check(event);

        // 获取Insert参数
        InsertParam insertParam = this.uxdfRdbConvert.makeEventInsertParam(event);
        final int insertNum = this.uxdfMapper.insertEvent(insertParam);
        event.set__Id(insertParam.getId());

        // TODO 保存后数据处理

        // 移除动态数据
        event.removeDynamicAttr();

        return insertNum;
    }

    /**
     * 更新Event实例{@link EventEntity}
     *
     * @param event Event实例
     * @return 更新成功数量
     */
    public int updateEvent(
            final EventEntity event,
            final SdData sdData
    ) {
        // 重新生成UUID
        event.generateUUID();
        // 统一设置更新时间
        event.set__UpdateTime(new Date());

        this.rdbValidator.check(event);

        // 获取update参数
        UpdateParam updateParam = this.uxdfRdbConvert.makeEventUpdateParam(event);

        final int updateNum = this.uxdfMapper.update(updateParam);

        // TODO 更新后数据处理

        // 移除动态数据
        event.removeDynamicAttr();

        return updateNum;
    }

    /**
     * 删除Event实例{@link EventEntity}
     *
     * @param event Event实例
     * @return 关联删除实例数量
     */
    public int deleteEvent(
            final EventEntity event,
            final SdData sdData
    ) {
        // 重新生成UUID
        event.generateUUID();
        final boolean enforce = event.isDeleteEnforce();
        // TODO：数据有效性判断，以及数据清洗

        final String eventId = event.get__Id();
        final String eventName = event.get__Sd();
        final String leftNodeName = event.get__LeftSd();
        final String rightNodeName = event.get__RightSd();

        // 获取定义
        final SdEventDefinition eventImpl = UXDFLoader.getEvent(eventName, leftNodeName, rightNodeName);

        final RelationShipRdbTable eventMapping = rdbLoader.getRdbEventMapping(eventName)
                .get(leftNodeName)
                .get(rightNodeName);
        final String eventTable = eventMapping.getName();

        final RelationShipRdbTable leftNodeMapping = rdbLoader.getRdbNodeMapping(leftNodeName);
        final String leftNodeTable = leftNodeMapping.getName();

        final RelationShipRdbTable rightNodeMapping = rdbLoader.getRdbNodeMapping(rightNodeName);
        final String rightNodeTable = rightNodeMapping.getName();

        int deletedNum = 0;


        // 当前Event对于左或右Node是必须
        if (eventImpl.getRequired() == SdEventRequiredType.right ||
                eventImpl.getRequired() == SdEventRequiredType.left ||
                eventImpl.getRequired() == SdEventRequiredType.both) {

            // 检查当前数据中是否没有和当前Event及必填Node相同的Event，没有则表示只是删除操作
            boolean findSame = false;
            if (sdData.containsEventSd(event.get__Sd())) {
                for (EventEntity eventEntity : sdData.getUnmodifiableEvent(event.get__Sd())) {
                    switch (eventImpl.getRequired()) {
                        case left:
                            findSame = eventEntity.get__Left().equals(event.get__Left())
                                    && eventEntity.get__LeftSd().equals(event.get__LeftSd());
                            break;
                        case right:
                            findSame = eventEntity.get__Right().equals(event.get__Right())
                                    && eventEntity.get__RightSd().equals(event.get__RightSd());
                            break;
                        case both:
                            findSame = eventEntity.get__Left().equals(event.get__Left())
                                    && eventEntity.get__LeftSd().equals(event.get__LeftSd())
                                    && eventEntity.get__Right().equals(event.get__Right())
                                    && eventEntity.get__RightSd().equals(event.get__RightSd());
                            break;
                    }
                    if (findSame) {
                        break;
                    }
                }
            }
            if (enforce) {
                // 可以强制删除
                // 先删除当前Event
                deletedNum += this.uxdfMapper.deleteEvent(
                        eventTable,
                        eventId,
                        null
                );

                // 未找到相同，进行删除
                if (!findSame) {
                    // 取出相关Node，依次删除
                    List<NodeEntity> needDeleteNodes = Lists.newArrayList();
                    // 查询结果
                    SdDataQueryResult queryResult;
                    switch (eventImpl.getRequired()) {
                        case left:
                            queryResult = this.getDataById(
                                    leftNodeName,
                                    event.get__Left()
                            );
                            if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                                needDeleteNodes.add(
                                        queryResult.getUxdf().getData().getUnmodifiableNode().get(0)
                                );
                            }
                            break;
                        case right:
                            queryResult = this.getDataById(
                                    leftNodeName,
                                    event.get__Right()
                            );
                            if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                                needDeleteNodes.add(
                                        queryResult.getUxdf().getData().getUnmodifiableNode().get(0)
                                );
                            }
                            break;
                        default:
                            queryResult = this.getDataById(
                                    leftNodeName,
                                    event.get__Left()
                            );
                            if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                                needDeleteNodes.add(
                                        queryResult.getUxdf().getData().getUnmodifiableNode().get(0)
                                );
                            }
                            queryResult = this.getDataById(
                                    leftNodeName,
                                    event.get__Right()
                            );
                            if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                                needDeleteNodes.add(
                                        queryResult.getUxdf().getData().getUnmodifiableNode().get(0)
                                );
                            }
                    }
                    log.debug("need delete nodes: {}", needDeleteNodes);
                    for (NodeEntity nodeEntity : needDeleteNodes) {
                        // 忽略不存在的Node
                        if (nodeEntity == null) {
                            continue;
                        }
                        nodeEntity.setOperateDeleteEnforce(Boolean.TRUE);
                        deletedNum += this.deleteNode(
                                nodeEntity,
                                sdData
                        );
                    }
                }

            } else { // 不可以强制删除
                if (findSame) {
                    // 找到相同，表示是一个更新可以只删除当前Event
                    deletedNum += this.uxdfMapper.deleteEvent(
                            eventTable,
                            eventId,
                            null
                    );
                } else {
                    // 未找到相同，提示异常
                    throw new UXDFSaveException(
                            messageSource.getMessage(
                                    "error.save.cascade.event",
                                    new Object[]{eventName, eventId, leftNodeName, rightNodeName},
                                    Locale.getDefault()
                            ),
                            SdOperateType.delete,
                            UXDFSaveErrorType.cascade
                    );
                }
            }

        } else { // 当前Event对于左右Node都不是必须，可以直接删除
            deletedNum += this.uxdfMapper.deleteEvent(
                    eventTable,
                    eventId,
                    null
            );
        }

        // TODO 删除后数据处理
        return deletedNum;
    }

    /**
     * 更新Event中的关联ID
     */
    private void updateEventId(final SdData sdData, final NodeEntity beforeNode, final String afterId) {
        final String beforeId = beforeNode.get__Id();
        List<EventEntity> eventEntities = sdData.getDetachedEvent(beforeNode);
        eventEntities.forEach(eventEntity -> {
            sdData.removeEvent(eventEntity);
            if (eventEntity.get__Right().equals(beforeId)) {
                eventEntity.set__Right(afterId);
            }
            if (eventEntity.get__Left().equals(beforeId)) {
                eventEntity.set__Left(afterId);
            }
            sdData.overwriteEvent(eventEntity);
        });
        beforeNode.set__Id(afterId);
    }
}
