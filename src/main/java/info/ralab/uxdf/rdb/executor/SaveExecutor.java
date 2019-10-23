package info.ralab.uxdf.rdb.executor;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import info.ralab.uxdf.SdData;
import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.UXDFLoader;
import info.ralab.uxdf.chain.ChainPath;
import info.ralab.uxdf.chain.UXDFChain;
import info.ralab.uxdf.chain.UXDFChainItem;
import info.ralab.uxdf.definition.*;
import info.ralab.uxdf.event.UXDFNodeChangeListener;
import info.ralab.uxdf.executor.Executor;
import info.ralab.uxdf.instance.EventEntity;
import info.ralab.uxdf.instance.IdMaker;
import info.ralab.uxdf.instance.NodeEntity;
import info.ralab.uxdf.model.SdDataQueryParam;
import info.ralab.uxdf.model.SdDataQueryRequest;
import info.ralab.uxdf.model.SdDataQueryResult;
import info.ralab.uxdf.model.SdDataSaveResult;
import info.ralab.uxdf.rdb.RdbStorageService;
import info.ralab.uxdf.rdb.convert.BinaryConvert;
import info.ralab.uxdf.rdb.exception.UXDFSaveErrorType;
import info.ralab.uxdf.rdb.exception.UXDFSaveException;
import info.ralab.uxdf.rdb.utils.FillDefaultValue;
import info.ralab.uxdf.rdb.utils.UXDFChangeListenerHelper;
import info.ralab.uxdf.utils.AssociateUniquePropertyUtil;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.context.MessageSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * {@link info.ralab.uxdf.SdData}保存执行者
 */
@Slf4j
public class SaveExecutor implements Executor<SdDataSaveResult> {

    private ApplicationContext applicationContext;
    private RdbStorageService storageService;
    private MessageSource messageSource;

    // 传入ID和保存后ID的映射MAP
    private Map<String, String> idMapping = Maps.newHashMap();
    // 待查询的Node
    private List<NodeEntity> queryNodes = Lists.newArrayList();
    // 待匹配的Node
    private List<NodeEntity> matchNodes = Lists.newArrayList();
    // 待新增或更新的Node
    private List<NodeEntity> createOrUpdateNodes = Lists.newArrayList();
    // 待新增的Node
    private List<NodeEntity> createNodes = Lists.newArrayList();
    // 待更新的Node
    private List<NodeEntity> updateNodes = Lists.newArrayList();
    // 待删除的Node
    private List<NodeEntity> deleteNodes = Lists.newArrayList();
    // 待新增的Event
    private List<EventEntity> createEvents = Lists.newArrayList();
    // 待更新的Event
    private List<EventEntity> updateEvents = Lists.newArrayList();
    // 待删除的Event
    private List<EventEntity> deleteEvents = Lists.newArrayList();
    // 级联的Sd标题
    private Set<String> cascadeSdTitle = Sets.newHashSet();
    // 需要级联删除的Sd Title
    private Map<String, Set<SdNodeDefinition>> cascadeDeleteTitle = Maps.newHashMap();
    // 需要保存的文件
    private UXDFBinaryFileInfo[] files;
    // 统一保存操作
    private SdOperateType operate;
    // 需要保存的数据
    private SdData data;
    // 保存结果
    private SdDataSaveResult saveResult;

    public SaveExecutor(
            final ApplicationContext applicationContext,
            final RdbStorageService storageService,
            final MessageSource messageSource,
            final SdData data
    ) {
        this.applicationContext = applicationContext;
        this.storageService = storageService;
        this.messageSource = messageSource;
        this.data = data;
        this.saveResult = new SdDataSaveResult();
    }

    /**
     * 保存的数据中二进制属性对应的文件
     *
     * @param files 文件数据
     * @return 执行类
     */
    public SaveExecutor files(final UXDFBinaryFileInfo[] files) {
        this.files = files;
        return this;
    }

    /**
     * 设置全局操作类型
     *
     * @param operate 操作类型
     * @return 执行类
     */
    public SaveExecutor operate(SdOperateType operate) {
        this.operate = operate;
        return this;
    }

    @Override
    public SdDataSaveResult execute() {
        // 替换Node集合
        List<NodeEntity> nodes = data.getDetachedNode();


        // 遍历需要保存的Node数据
        log.debug("each node start:{}", System.currentTimeMillis());
        AtomicLong bySaveTime = new AtomicLong();
        nodes.forEach(nodeEntity -> {
            // 跳过空数据
            if (nodeEntity == null || nodeEntity.isEmpty()) {
                return;
            }

            // TODO 数据权限过滤

            // 数据保存事件
            UXDFNodeChangeListener nodeChangeListener = UXDFChangeListenerHelper
                    .getNodeChangeListener(this.applicationContext, nodeEntity.get__Sd());
            if (nodeChangeListener != null) {
                nodeChangeListener.save(nodeEntity, data);
            }


            // 获取操作类型
            SdOperateType operate = nodeEntity.getOperate();
            if (operate == null) {
                if (this.operate == null) {
                    // 没有操作类型，不做操作。直接跳过
                    return;
                } else {
                    // 设置统一操作
                    operate = this.operate;
                    nodeEntity.setOperate(operate);
                }
            }

            // 根据不同操作类型进行数据过滤
            switch (operate) {
                case create: // 新增
                    createNodes.add(nodeEntity);
                    break;
                case update: // 更新
                    updateNodes.add(nodeEntity);
                    break;
                case delete: // 删除
                    deleteNodes.add(nodeEntity);
                    // 未标记强制删除，校验关联Sd
                    String sdName = nodeEntity.get__Sd();
                    // 未被检测的SD 并且 非强制删除对象
                    if (!cascadeSdTitle.contains(sdName) && !nodeEntity.isDeleteEnforce()) {
                        cascadeSdTitle.add(sdName);
                        SdNodeDefinition nodeDefinition = UXDFLoader.getNode(sdName);
                        String title = nodeDefinition.getTitle();
                        // 获取所有级联Node的定义
                        Set<SdNodeDefinition> cascadeSdNode = Sets.newLinkedHashSet();
                        cascadeSdNode.add(nodeDefinition);
                        this.fillCascadeData(sdName, cascadeSdNode);
                        cascadeSdNode.remove(nodeDefinition);
                        if (!cascadeSdNode.isEmpty()) {
                            cascadeDeleteTitle.putIfAbsent(title, cascadeSdNode);
                        }
                    }
                    break;
                case query: // 查询
                    // 查询可以保留原有版本库信息
                    queryNodes.add(nodeEntity);
                    break;
                case match: // 匹配
                    // 匹配可以保留原有版本库信息
                    matchNodes.add(nodeEntity);
                    break;
                case createOrUpdate: // 新增或更新
                    // 设置版本分支信息
                    createOrUpdateNodes.add(nodeEntity);
                    break;
                default: // 未知 不做操作重新放回数据集合
                    data.addNodeIfAbsent(nodeEntity);
            }
        });
        log.debug("by save time:{}", bySaveTime.get());
        log.debug("each node end:{}", System.currentTimeMillis());
        // 拼接提示信息
        if (!cascadeDeleteTitle.isEmpty()) {
            log.debug("delete title start:{}", System.currentTimeMillis());
            StringBuilder message = new StringBuilder();
            cascadeDeleteTitle.forEach((title, cascadeSdNodes) -> {
                List<SdNodeDefinition> sdNodes = cascadeSdNodes.stream().filter(
                        // 只输出user相关数据
                        sdNode -> sdNode.isNamespace("ns_user")
                ).collect(Collectors.toList());
                if (sdNodes.size() != 0) {
                    message.append(messageSource.getMessage(
                            "error.save.cascade",
                            new Object[]{title, Joiner.on(",").join(sdNodes.stream().map(SdNodeDefinition::getTitle).collect(Collectors.toList()))},
                            Locale.getDefault()
                    )).append("\n");
                }
            });
            log.debug("delete title end:{}", System.currentTimeMillis());
            throw new UXDFSaveException(message.toString(), SdOperateType.delete, UXDFSaveErrorType.cascade);
        }

        // 处理Node
        List<NodeEntity> waitQueryNodes = Lists.newArrayList();
        List<NodeEntity> waitMatchNodes = Lists.newArrayList();
        List<NodeEntity> waitCreateOrUpdateNodes = Lists.newArrayList();
        List<NodeEntity> waitUpdateNodes = Lists.newArrayList();
        List<NodeEntity> waitCreateNodes = Lists.newArrayList();

        log.debug("save node start:{}", System.currentTimeMillis());
        while (!queryNodes.isEmpty() ||
                !matchNodes.isEmpty() ||
                !createOrUpdateNodes.isEmpty() ||
                !updateNodes.isEmpty() ||
                !createNodes.isEmpty() ||
                !deleteNodes.isEmpty()) {
            int handleNum = queryNodes.size() + matchNodes.size() + createOrUpdateNodes.size() + updateNodes.size() + createNodes.size();
            // 查询
            queryNodes.forEach(nodeEntity -> {
                final String logicId = nodeEntity.getLogicId();
                final String nodeId = nodeEntity.get__Id();

                NodeEntity existNode = this.storageService.loadNodeByEntity(nodeEntity, false);
                // 未找到抛出异常
                if (existNode == null) {
                    throw new UXDFException(String.format("Node %s can't match any record.", nodeEntity.toJSONString()));
                }
                // 记录临时ID
                idMapping.put(nodeId, existNode.get__Id());
                data.updateNode(logicId, nodeId, existNode);

                // 更新Event中关联的Id
                // updateEventId(nodeEntity, nodeId, existEntity.get__Id());
            });
            // 匹配
            matchNodes.forEach(nodeEntity -> {
                final String logicId = nodeEntity.getLogicId();
                final String nodeId = nodeEntity.get__Id();
                // 填充冗余属性值失败，需要等待剩余数据处理结束
                if (!fillAssociateUniquePropertyValued(nodeEntity)) {
                    waitMatchNodes.add(nodeEntity);
                    return;
                }

                NodeEntity existNode = this.storageService.loadNodeByEntity(nodeEntity, true);
                // 未找到抛出异常
                if (existNode == null) {
                    throw new UXDFException(String.format("Node %s can't match any record.", nodeEntity.toJSONString()));
                }
                // 记录临时ID
                idMapping.put(nodeId, existNode.get__Id());
                data.updateNode(logicId, nodeId, existNode);
            });

            // 新增或更新
            createOrUpdateNodes.forEach(nodeEntity -> {
                final String logicId = nodeEntity.getLogicId();
                final String nodeId = nodeEntity.get__Id();
                // 填充冗余属性值失败，需要等待剩余数据处理结束
                if (!fillAssociateUniquePropertyValued(nodeEntity)) {
                    waitCreateOrUpdateNodes.add(nodeEntity);
                    return;
                }

                NodeEntity existNode = this.storageService.loadNodeByEntity(nodeEntity, true);
                if (existNode == null) { // 不存在相同Node，新增
                    nodeEntity.setOperate(SdOperateType.create);
                    createNodes.add(nodeEntity);
                } else { // 存在相同Node，更新
                    nodeEntity.setOperate(SdOperateType.update);
                    // 替换ID，记录ID映射关系
                    nodeEntity.set__Id(existNode.get__Id());
                    idMapping.put(nodeId, nodeEntity.get__Id());
                    updateNodes.add(nodeEntity);
                    data.updateNode(logicId, nodeId, existNode);
                }
            });

            // 更新Node
            updateNodes.forEach(nodeEntity -> {
                final String logicId = nodeEntity.getLogicId();
                final String nodeId = nodeEntity.get__Id();
                // 填充冗余属性值失败，需要等待剩余数据处理结束
                if (!fillAssociateUniquePropertyValued(nodeEntity)) {
                    waitUpdateNodes.add(nodeEntity);
                    return;
                }

                // 填充默认值失败，需要等待剩余数据处理结束
                if (!fillDefaultValue(nodeEntity)) {
                    waitUpdateNodes.add(nodeEntity);
                    return;
                }

                // 如果有二进制文件
                if (this.files != null && this.files.length > 0) {
                    nodeEntity.put(BinaryConvert.SD_PROP_FILES, files);
                }

                // 更新前事件
                UXDFNodeChangeListener nodeChangeListener = UXDFChangeListenerHelper
                        .getNodeChangeListener(this.applicationContext, nodeEntity.get__Sd());
                if (nodeChangeListener != null) {
                    nodeChangeListener.update(nodeEntity, data);
                }

                // 更新Node
                int updateNum = this.storageService.updateNode(nodeEntity, this.data);
                this.saveResult.getNodeUpdateNum().set(updateNum);
                data.updateNode(logicId, nodeId, nodeEntity);

                // 更新后事件
                if (nodeChangeListener != null) {
                    nodeChangeListener.updated(nodeEntity, data);
                }
            });
            // 新增Node
            createNodes.forEach(nodeEntity -> {
                final String logicId = nodeEntity.getLogicId();
                final String nodeId = nodeEntity.get__Id();
                // 填充冗余属性值失败，需要等待剩余数据处理结束
                if (!fillAssociateUniquePropertyValued(nodeEntity)) {
                    waitCreateNodes.add(nodeEntity);
                    return;
                }
                // 填充默认值失败，需要等待剩余数据处理结束
                if (!fillDefaultValue(nodeEntity)) {
                    waitCreateNodes.add(nodeEntity);
                    return;
                }
                // 如果有二进制文件
                if (this.files != null && this.files.length > 0) {
                    nodeEntity.put(BinaryConvert.SD_PROP_FILES, files);
                }

                // 新增前事件
                UXDFNodeChangeListener nodeChangeListener = UXDFChangeListenerHelper
                        .getNodeChangeListener(this.applicationContext, nodeEntity.get__Sd());
                if (nodeChangeListener != null) {
                    nodeChangeListener.create(nodeEntity, data);
                }

                // 新增Node
                int insertNum = this.storageService.createNode(nodeEntity, this.data);
                saveResult.getNodeCreateNum().set(insertNum);

                // 记录ID映射
                idMapping.put(nodeId, nodeEntity.get__Id());

                data.updateNode(logicId, nodeId, nodeEntity);

                // 新增后事件
                if (nodeChangeListener != null) {
                    nodeChangeListener.created(nodeEntity, data);
                }
            });
            deleteNodes.forEach(nodeEntity -> {

                // 删除前事件
                UXDFNodeChangeListener nodeChangeListener = UXDFChangeListenerHelper
                        .getNodeChangeListener(this.applicationContext, nodeEntity.get__Sd());
                if (nodeChangeListener != null) {
                    nodeChangeListener.delete(nodeEntity, data);
                }

                // 删除Node
                int deleteNum = this.storageService.deleteNode(
                        nodeEntity, this.data
                );
                saveResult.getNodeDeleteNum().set(deleteNum);
                data.removeNode(nodeEntity);

                // 删除后事件
                if (nodeChangeListener != null) {
                    nodeChangeListener.deleted(nodeEntity, data);
                }
            });

            int waitNum = waitQueryNodes.size() +
                    waitMatchNodes.size() +
                    waitCreateOrUpdateNodes.size() +
                    waitCreateNodes.size() +
                    waitUpdateNodes.size();

            if (log.isDebugEnabled()) {
                log.debug("create node size: {}", handleNum);
                log.debug("wait query node size: {}", waitQueryNodes.size());
                log.debug("wait match node size: {}", waitMatchNodes.size());
                log.debug("wait createOrUpdate node size: {}", waitCreateOrUpdateNodes.size());
                log.debug("wait create node size: {}", waitCreateNodes.size());
                log.debug("wait update node size: {}", waitUpdateNodes.size());
            }

            // 未处理数大于等于处理总数，表明需要填充的数据的无法获得
            if (waitNum > 0 && waitNum >= handleNum) {
                throw new UXDFException(
                        String.format(
                                "These node can't fill value. \n%s\n%s\n%s\n%s\n%s",
                                waitQueryNodes.size(),
                                waitMatchNodes.size(),
                                waitCreateOrUpdateNodes.size(),
                                waitCreateNodes.size(),
                                waitUpdateNodes.size()
                        )
                );

            }

            // 清空当前集合，将等待处理数据加入再次处理
            queryNodes.clear();
            if (!waitQueryNodes.isEmpty()) {
                queryNodes.addAll(waitQueryNodes);
                waitQueryNodes.clear();
            }
            matchNodes.clear();
            if (!waitMatchNodes.isEmpty()) {
                matchNodes.addAll(waitMatchNodes);
                waitMatchNodes.clear();
            }
            createOrUpdateNodes.clear();
            if (!waitCreateOrUpdateNodes.isEmpty()) {
                createOrUpdateNodes.addAll(waitCreateOrUpdateNodes);
                waitCreateOrUpdateNodes.clear();
            }
            createNodes.clear();
            if (!waitCreateNodes.isEmpty()) {
                createNodes.addAll(waitCreateNodes);
                waitCreateNodes.clear();
            }
            updateNodes.clear();
            if (!waitUpdateNodes.isEmpty()) {
                updateNodes.addAll(waitUpdateNodes);
                waitUpdateNodes.clear();
            }
            deleteNodes.clear();
        }
        log.debug("save node end:{}", System.currentTimeMillis());

        // 处理Event集合
        Map<String, List<EventEntity>> eventMap = data.getDetachedEvent();

        // 遍历需要保存的Event数据
        eventMap.forEach((eventName, eventEntities) -> {
            // 遍历每一类Event集合
            eventEntities.forEach(eventEntity -> {
                // 跳过空数据
                if (eventEntity == null || eventEntity.isEmpty()) {
                    return;
                }
                // 获取操作类型
                SdOperateType operate = eventEntity.getOperate();
                if (operate == null) {
                    if (this.operate == null) {
                        // 没有操作类型，不做操作。跳过
                        return;
                    } else {
                        // 设置统一操作
                        operate = this.operate;
                        eventEntity.setOperate(operate);
                    }
                }

                // 根据不同操作类型进行数据过滤，Event目前暂时不支持query、match、createOrUpdate操作
                switch (operate) {
                    case create: // 新增
                        createEvents.add(eventEntity);
                        break;
                    case update: // 更新
                        updateEvents.add(eventEntity);
                        break;
                    case delete: // 删除
                        deleteEvents.add(eventEntity);
                        break;
                    case query: // 查询
                    case match: // 匹配
                    case createOrUpdate: // 新增或更新
                    default: // 未知，理论不会出现
                }
            });
        });

        // 新增Event
        createEvents.forEach(eventEntity -> {
            final String logicId = eventEntity.getLogicId();
            final String eventId = eventEntity.get__Id();

            // 保存Event
            this.storageService.createEvent(eventEntity, this.data);
            data.updateEvent(logicId, eventId, eventEntity);
        });
        // 更新Event
        updateEvents.forEach(eventEntity -> {
            final String logicId = eventEntity.getLogicId();
            final String eventId = eventEntity.get__Id();

            // 更新Event
            this.storageService.updateEvent(eventEntity, this.data);
            data.updateEvent(logicId, eventId, eventEntity);
        });
        // 删除Event
        deleteEvents.forEach(eventEntity -> {
            this.storageService.deleteEvent(eventEntity, this.data);
            data.removeEvent(eventEntity);
        });

        return saveResult;
    }

    /**
     * 填充默认值
     *
     * @param node Node实例
     * @return 是否完成填充
     */
    private boolean fillDefaultValue(final NodeEntity node) {
        SdNodeDefinition sdNode = UXDFLoader.getNode(node.get__Sd());

        for (Map.Entry<String, SdProperty> entry : sdNode.getProp().entrySet()) {
            String propName = entry.getKey();
            SdProperty prop = entry.getValue();
            Object propValue = node.get(propName);
            Object defaultValue = prop.getDefaultValue();

            // 属性有值 或 没有默认值 跳过
            if (propValue != null || defaultValue == null) {
                continue;
            }


            // 填充默认值
            FillDefaultValue fillDefaultValue = FillDefaultValue.getInstance(defaultValue);
            if (fillDefaultValue == null) {
                // 普通默认值
                node.putIfAbsent(propName, defaultValue);
                continue;
            }

            // 获取关联默认值
            UXDFChain chain = fillDefaultValue.getChain();
            // 通过语法链获取默认值
            List<UXDFChainItem> items = chain.iterator().next();
            if (items.isEmpty()) {
                return false;
            }

            // 目标Node
            NodeEntity targetNode = node;
            // 源Node
            NodeEntity sourceNode;
            for (UXDFChainItem chainItem : items) {
                String eventName = chainItem.getEvent();
                // 将源替换为目标
                sourceNode = targetNode;
                // 在当前数据中查找
                List<EventEntity> eventEntities = this.data.getDetachedEvent(sourceNode);
                // 记录符合串的Event实例
                EventEntity matchedEvent = null;
                for (EventEntity eventEntity : eventEntities) {
                    if (eventEntity.get__Sd().equals(eventName)) {
                        if (chainItem.getChainPath() == ChainPath.LEFT
                                && eventEntity.get__RightSd().equals(chainItem.getLastNode())) {
                            matchedEvent = eventEntity;
                            targetNode = this.data.getNodeByLogicId(eventEntity.rightLogicId());
                        } else if (chainItem.getChainPath() == ChainPath.RIGHT
                                && eventEntity.get__LeftSd().equals(chainItem.getLastNode())) {
                            matchedEvent = eventEntity;
                            targetNode = this.data.getNodeByLogicId(eventEntity.leftLogicId());
                        }
                        // 如果已找到，则跳出循环
                        if (targetNode != null) {
                            break;
                        }
                    }
                }
                // 在数据库中查找
                if (targetNode == null) {
                    targetNode = queryNodeEntityByChainItem(matchedEvent, chainItem);
                }
            }

            if (targetNode == null) { // 未找到目标
                return false;
            } else { // 找到目标
                if (IdMaker.effective(targetNode.get__Id())) {
                    node.put(propName, targetNode.get(fillDefaultValue.getProperty()));
                } else {
                    // 还未保存，需要等待保存
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 填充冗余属性值
     *
     * @param node Node实例
     * @return 是否完成填充冗余属性
     */
    private boolean fillAssociateUniquePropertyValued(final NodeEntity node) {

        // 获取冗余属性名称
        SdDefinition sdDefinition = UXDFLoader.getNode(node.get__Sd());
        String redundancyPropertyName = AssociateUniquePropertyUtil.getPropertyName(sdDefinition);
        if (redundancyPropertyName == null) {
            return true;
        }

        // 获取语法链
        UXDFChainItem uxdfChainItem = null;
        String[] uniqueIndexs = sdDefinition.getUniqueIndex();
        for (String uniqueIndex : uniqueIndexs) {
            if (UXDFChain.haveRelationship(uniqueIndex)) {
                UXDFChain uxdfChain = UXDFChain.getInstance(uniqueIndex);
                List<UXDFChainItem> items = uxdfChain.iterator().next();
                uxdfChainItem = items.get(0);
                break;
            }
        }
        if (uxdfChainItem == null) {
            throw new UXDFException(String.format("[%s] 定义中的唯一属性有误", node.get__Sd()));
        }

        // 目标Node
        NodeEntity targetNode = node;
        // 源Node
        NodeEntity sourceNode;
        // 将源替换为目标
        sourceNode = targetNode;
        // 清空目标
        targetNode = null;

        // 在当前数据中查找
        String eventName = uxdfChainItem.getEvent();
        List<EventEntity> eventEntities = this.data.getDetachedEvent(sourceNode);
        // 记录符合串的Event实例
        EventEntity matchedEvent = null;
        for (EventEntity eventEntity : eventEntities) {
            // 跳过不是当前Event的实例
            if (!eventEntity.get__Sd().equals(eventName)) {
                continue;
            }
            if (ChainPath.LEFT == uxdfChainItem.getChainPath()
                    && eventEntity.get__RightSd().equals(uxdfChainItem.getRightNodeName())) {
                // 唯一关联的Chain从左向右
                matchedEvent = eventEntity;
                targetNode = this.data.getNodeByLogicId(eventEntity.rightLogicId());
            } else if (ChainPath.RIGHT == uxdfChainItem.getChainPath()
                    && eventEntity.get__LeftSd().equals(uxdfChainItem.getLeftNodeName())) {
                // 唯一关联的Chain从右向左
                matchedEvent = eventEntity;
                targetNode = this.data.getNodeByLogicId(eventEntity.leftLogicId());
            }
            // 如果已找到，则跳出循环
            if (targetNode != null) {
                break;
            }
        }

        // 表示当前不更新关联唯一属性
        if (matchedEvent == null) {
            return true;
        }

        // 在数据库中查找
        if (targetNode == null) {
            targetNode = queryNodeEntityByChainItem(matchedEvent, uxdfChainItem);
        }

        if (targetNode == null) {
            // 未找到目标
            return false;
        } else { // 找到目标
            if (IdMaker.effective(targetNode.get__Id())) {
                node.put(redundancyPropertyName, targetNode.get__Id());
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * 通过{@link UXDFChainItem}和Event{@link EventEntity}获取LastNode{@link NodeEntity}
     *
     * @param eventEntity 关系实例
     * @param chainItem   UXDF串
     * @return 结束Node实例
     */
    private NodeEntity queryNodeEntityByChainItem(final EventEntity eventEntity, final UXDFChainItem chainItem) {
        if (eventEntity == null || chainItem == null || !eventEntity.get__Sd().equals(chainItem.getEvent())) {
            return null;
        }
        final String lastNodeLabel = "last";
        chainItem.setLastLabel(lastNodeLabel);
        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
        queryRequest.getChains().add(chainItem.getLastNode());
        if (chainItem.getChainPath().equals(ChainPath.LEFT)) {
            queryRequest.getParams().put(chainItem.getLastNode(), Lists.newArrayList(
                    SdDataQueryParam.equal(NodeEntity.ATTR_ID, eventEntity.get__Right()),
                    SdDataQueryParam.equal(NodeEntity.ATTR_SD, eventEntity.get__RightSd())
            ));
        } else if (chainItem.getChainPath().equals(ChainPath.RIGHT)) {
            queryRequest.getParams().put(chainItem.getLastNode(), Lists.newArrayList(
                    SdDataQueryParam.equal(NodeEntity.ATTR_ID, eventEntity.get__Left()),
                    SdDataQueryParam.equal(NodeEntity.ATTR_SD, eventEntity.get__LeftSd())
            ));
        }
        // 只返回目标Node
        queryRequest.getReturns().add(chainItem.getLastNode());
        SdDataQueryResult queryResult = this.storageService.makeQueryRequestExecutor(queryRequest).execute();

        if (queryResult.getUxdf().getData().isNodeEmpty()) {
            return null;
        } else {
            return queryResult.getUxdf().getData().getUnmodifiableNode().get(0);
        }
    }

    /**
     * 填充Node名称关联的所有Node定义
     *
     * @param nodeName  Node名称
     * @param cascadeSd 需要填充的Node定义集合
     */
    private void fillCascadeData(final String nodeName, final Set<SdNodeDefinition> cascadeSd) {

        // 该Node关联的Event
        Set<SdEventDefinition> sdEvents = UXDFLoader.getEventsByNodeName(nodeName);
        if (sdEvents == null) {
            return;
        }
        for (SdEventDefinition eventImpl : sdEvents) {
            final String leftNodeName = eventImpl.getLeftNodeName();
            final String rightNodeName = eventImpl.getRightNodeName();
            /*
            TODO 如果当前Node是Master 并且 当前Event还未查找过 则继续查找
            TODO 如果当前当前Node不是Master 并且 有任意一个Event被查找过 则不继续
             */

            if (nodeName.equals(leftNodeName)
                    && (eventImpl.getRequired() == SdEventRequiredType.right
                    || eventImpl.getRequired() == SdEventRequiredType.both)) { // 当前Node是左节点，event对于右节点必须
                SdNodeDefinition sdNode = UXDFLoader.getNode(rightNodeName);
                // 如果已经在级联列表中，则跳过
                if (cascadeSd.contains(sdNode)) {
                    continue;
                }
                cascadeSd.add(sdNode);
                this.fillCascadeData(rightNodeName, cascadeSd);
            } else if (nodeName.equals(rightNodeName)
                    && (eventImpl.getRequired() == SdEventRequiredType.left
                    || eventImpl.getRequired() == SdEventRequiredType.both)) { // 当前Node是右节点，event对于左节点必须
                SdNodeDefinition sdNode = UXDFLoader.getNode(leftNodeName);
                // 如果已经在级联列表中，则跳过
                if (cascadeSd.contains(sdNode)) {
                    continue;
                }
                cascadeSd.add(sdNode);
                this.fillCascadeData(leftNodeName, cascadeSd);
            }
        }
    }
}
