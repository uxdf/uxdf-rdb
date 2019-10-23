package info.ralab.uxdf.rdb.executor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.SdData;
import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.UXDFLoader;
import info.ralab.uxdf.chain.ChainPath;
import info.ralab.uxdf.chain.UXDFChain;
import info.ralab.uxdf.chain.UXDFChainItem;
import info.ralab.uxdf.definition.SdBaseType;
import info.ralab.uxdf.definition.SdEventDefinition;
import info.ralab.uxdf.definition.SdNodeDefinition;
import info.ralab.uxdf.definition.SdProperty;
import info.ralab.uxdf.executor.Executor;
import info.ralab.uxdf.instance.*;
import info.ralab.uxdf.model.*;
import info.ralab.uxdf.rdb.DataAuth;
import info.ralab.uxdf.rdb.RdbLoader;
import info.ralab.uxdf.rdb.mapper.UXDFQueryMapper;
import info.ralab.uxdf.rdb.model.*;
import info.ralab.uxdf.rdb.utils.UXDFRdbConvert;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 查询请求执行者
 */
public class QueryRequestExecutor implements Executor<SdDataQueryResult> {
    /**
     * 表别名序列
     */
    private AtomicLong tableSeq = new AtomicLong();
    /**
     * 列别名序列
     */
    private AtomicLong columnSeq = new AtomicLong();
    /**
     * 相同标签下可能存在不同的Sd。记录别名和标签
     */
    private Map<String, String> labelSdMapping = Maps.newConcurrentMap();
    /**
     * 查询信息集合，用于生成关联查询
     */
    private List<RdbQueryInfo> queryInfoList = Lists.newArrayList();

    private SdDataQueryResult queryResult;

    private RdbLoader rdbLoader;
    private UXDFRdbConvert uxdfRdbConvert;
    private UXDFQueryMapper uxdfQueryMapper;
    private SdDataQueryRequest queryRequest;

    public QueryRequestExecutor(
            final RdbLoader rdbLoader,
            final UXDFRdbConvert uxdfRdbConvert,
            final UXDFQueryMapper uxdfQueryMapper,
            final SdDataQueryRequest queryRequest
    ) {
        this.rdbLoader = rdbLoader;
        this.uxdfRdbConvert = uxdfRdbConvert;
        this.uxdfQueryMapper = uxdfQueryMapper;
        this.queryRequest = queryRequest;
    }

    @Override
    public SdDataQueryResult execute() {
        // 构建基本返回结果
        queryResult = new SdDataQueryResult(UXDFLoader.getBaseUXDF());

        // 如果查询请求为空，返回空
        if (this.queryRequest == null) {
            return queryResult;
        }

        /*
         * 解析查询关系串，形成树形结构
         */
        SdData chainData = new SdData();
        // 保存Sd的标签和ID对应关系
        Map<String, String> sdLabelIdMap = Maps.newConcurrentMap();
        // 遍历关系链并将Sd定义添加入SdData用于构建tree结构
        queryRequest.getChains().forEach(chainString -> {
            // 如果符合关系串
            if (UXDFChain.haveRelationship(chainString)) {
                // 生成关系链
                UXDFChain chain = UXDFChain.getInstance(chainString);
                // 将内容填充到SdData
                this.fillChainToSdData(chain, chainData, sdLabelIdMap);
                // 获取标签和Sd映射关系
                chain.getLabelSdMapping().forEach((label, sd) -> {
                    // 检查Sd名称是否正确
                    if (UXDFLoader.getNode(sd) == null && UXDFLoader.getEvent(sd) == null) {
                        throw new UXDFException(String.format(
                                "Sd名称[%s]未定义。", sd
                        ));
                    }
                    // 检查映射关系是否已经存在
                    labelSdMapping.compute(label, (key, value) -> {
                        // 对应当前标签的value不存在 或 和当前标签及Sd名称完全一致 认为正确
                        if (value == null || sd.equals(value)) {
                            return sd;
                        } else {
                            throw new UXDFException(String.format(
                                    "标签[%s]在查询请求中定义不明确。", label
                            ));
                        }
                    });
                });
            } else {
                // 单独Node定义
                UXDFChainItem chainItem = new UXDFChainItem();
                chainItem.addSd(chainString);

                final String label = chainItem.getFirstLabel();
                final String sd = chainItem.getFirstNode();

                // 检查Sd名称是否正确
                if (UXDFLoader.getNode(sd) == null) {
                    throw new UXDFException(String.format(
                            "Sd名称[%s]未定义。", sd
                    ));
                }

                // 加入SdData中
                if (!this.labelSdMapping.containsKey(label)) {
                    // 还未包含
                    NodeEntity labelNodeEntity = new NodeEntity();
                    labelNodeEntity.id(IdMaker.temp()).sd(label);

                    chainData.addNodeIfAbsent(labelNodeEntity);
                    sdLabelIdMap.put(label, labelNodeEntity.get__Id());
                    this.labelSdMapping.put(label, sd);
                } else if (!sd.equals(this.labelSdMapping.get(label))) {
                    // 对应的Sd和已有标签不匹配
                    throw new UXDFException(String.format(
                            "标签[%s]在查询请求中定义不明确。",
                            label
                    ));
                }
            }
        });

        // 设置起始解析位置
        NodeEntity mainNode;
        if (queryRequest.hasOrder() || queryRequest.hasPage()) {
            // 如果有分页和排序
            Optional<NodeEntity> matchedNode = chainData.getUnmodifiableNode()
                    .stream()
                    .filter(nodeEntity -> nodeEntity.get__Sd().equals(queryRequest.getMain().getAlias()))
                    .findFirst();
            if (matchedNode.isPresent()) {
                mainNode = matchedNode.get();
            } else {
                // 未找到分页或排序的标签指向的Sd
                throw new UXDFException(String.format(
                        "在关系链中未找到分页或排序使用的标签[%s]。",
                        queryRequest.getMain().getAlias()
                ));
            }

            // 创建分页返回信息
            queryResult.setMainSize(new SdDataQuerySize());
        } else {
            // 没有分页排序，使用默认第一个
            mainNode = chainData.getUnmodifiableNode().get(0);
        }

        // 从mainNode开始构建树形查询结构
        RdbQueryInfo queryInfo = makeQueryInfo(mainNode, chainData);
        // 合并exists条件
        if (queryInfo.getExists() != null) {
            queryInfo.getExists().mergeChildren();
        }

        // 是否只查询main和相关条件，不join其它数据结果
        if (queryRequest.isOnlyMain()) {
            queryInfo.setJoin(null);
        }

        // 需要分页结果
        if (this.queryRequest.hasPage()) {
            // 设置分页总数
            queryResult.getMainSize().setCount(
                    this.uxdfQueryMapper.count(this.queryInfoList.get(0))
            );
        }

        // 需要排序
        if (this.queryRequest.hasOrder()) {
            // 更新排序列别名
            final String nodeName = queryInfo.getNodeDefinition().getNodeName();
            RelationShipRdbTable rdbTable = this.rdbLoader.getRdbNodeMapping(nodeName);
            this.queryRequest.getMain().getOrders().forEach(sdDataQueryOrder -> {
                final String orderColumn = rdbTable.getColumn().get(sdDataQueryOrder.getProperty());
                // 为主查询信息添加排序
                queryInfo.getOrders().add(
                        new SdDataQueryOrder(orderColumn, sdDataQueryOrder.getType())
                );
                // 替换外层排序字段
                final String orderColumnLabel = queryInfo.getColumns().inverse().get(orderColumn);
                sdDataQueryOrder.setProperty(orderColumnLabel);
            });
        }

        // 查询数据
        boolean queryCountIsZero = (
                this.queryRequest.hasPage() && this.queryResult.getMainSize().getCount() == 0
        );

        // todo 判断权限
        DataAuth dataAuth = null;
        List<JSONObject> queryData = queryCountIsZero ?
                Lists.newArrayList() : this.uxdfQueryMapper.query(
                this.queryInfoList,
                this.queryRequest.hasOrder() ? this.queryRequest.getMain().getOrders() : null,
                this.queryRequest.hasPage() ? this.queryRequest.getMain().getPage() : null,
                dataAuth
        );

        // 非主Node结果集合
        List<NodeEntity> notMainSdNode = Lists.newArrayList();

        queryData.forEach(queryDataResult -> {
            // 这里重新用一个JSON接收是为了防止污染缓存数据
            JSONObject rdbResult = new JSONObject();
            rdbResult.putAll(queryDataResult);
            // 遍历所有查询信息，重新组装数据
            this.queryInfoList.forEach(rdbQueryInfo -> {
                String label = rdbQueryInfo.getLabel();
                // 不在返回结果中，不处理
                if (!this.queryRequest.getReturns().isEmpty()
                        && !this.queryRequest.getReturns().contains(label)) {
                    return;
                }
                // 从数据库结果中，将别名替换为列名
                rdbQueryInfo.getColumns().forEach((alias, column) -> rdbResult.put(column, rdbResult.remove(alias)));

                if (rdbQueryInfo.isNodeEntity()) {
                    // 如果是Node
                    NodeEntity nodeEntity = this.uxdfRdbConvert.rdbToNode(rdbResult);
                    if (mainNode.get__Sd().equals(label)) {
                        // 是主Node
                        queryResult.getUxdf().getData().addNodeIfAbsent(nodeEntity);
                    } else {
                        // 不是主Node
                        notMainSdNode.add(nodeEntity);
                    }
                } else {
                    // 如果是Node
                    EventEntity eventEntity = this.uxdfRdbConvert.rdbToEvent(rdbResult);
                    queryResult.getUxdf().getData().addEventIfAbsent(eventEntity);
                }
            });
        });

        // 需要分页结果
        if (this.queryRequest.hasPage()) {
            // 设置当前分页Node返回数量
            queryResult.getMainSize().setCurrent(
                    queryResult.getUxdf().getData().isNodeEmpty() ?
                            0 : queryResult.getUxdf().getData().getUnmodifiableNode().size()
            );
        }

        notMainSdNode.forEach(nodeEntity -> queryResult.getUxdf().getData().addNodeIfAbsent(nodeEntity));


        return queryResult;

    }

    /**
     * 基于{@link NodeEntity}生成查询信息{@link RdbQueryInfo}
     *
     * @param startNode node实例
     * @param chainData 关系链数据
     * @return 查询信息
     */
    private RdbQueryInfo makeQueryInfo(
            final NodeEntity startNode,
            final SdData chainData
    ) {
        // 起始Node别名
        final String startNodeLabel = startNode.get__Sd();
        final String startNodeName = this.labelSdMapping.get(startNodeLabel);
        // 起始Node定义
        SdNodeDefinition startNodeDefinition = UXDFLoader.getNode(startNodeName);
        // 起始Node数据库映射关系
        RelationShipRdbTable startNodeRdbTable = this.rdbLoader.getRdbNodeMapping(startNodeName);

        // 创建起始Node对应的查询信息
        RdbQueryInfo queryInfo = makeQueryInfoByNode(startNodeDefinition, startNodeRdbTable, startNodeLabel);

        // 加入查询信息集合
        this.queryInfoList.add(queryInfo);


        // 遍历当前Node的所有Event
        chainData.getDetachedEvent(startNode).forEach(eventEntity -> {
            final String eventLabel = eventEntity.get__Sd();
            final String eventName = labelSdMapping.get(eventLabel);

            final String leftNodeLabel = eventEntity.get__LeftSd();
            final String leftNodeName = labelSdMapping.get(leftNodeLabel);

            final String rightNodeLabel = eventEntity.get__RightSd();
            final String rightNodeName = labelSdMapping.get(rightNodeLabel);

            final SdEventDefinition eventDefinition = UXDFLoader.getEvent(
                    eventName,
                    leftNodeName,
                    rightNodeName
            );
            final RelationShipRdbTable eventRdbTable = this.rdbLoader.getRdbEventMapping(
                    eventName,
                    leftNodeName,
                    rightNodeName
            );

            // 当前Event的关系串
            final String eventChain = String.format(
                    "%s-%s>%s",
                    leftNodeLabel,
                    eventLabel,
                    rightNodeLabel
            );

            final NodeEntity leftNode = chainData.getNodeByLogicId(eventEntity.leftLogicId());
            final NodeEntity rightNode = chainData.getNodeByLogicId(eventEntity.rightLogicId());
            ChainPath chainPath = null;

            // 下一个Node
            NodeEntity nextNode = null;

            // 关系的左Node是下一个Node
            if (checkIsNext(startNode, leftNode, eventEntity, eventChain)) {
                chainPath = ChainPath.RIGHT;
                nextNode = leftNode;
            }

            // 关系的右Node是下一个Node
            if (checkIsNext(startNode, rightNode, eventEntity, eventChain)) {
                chainPath = ChainPath.LEFT;
                nextNode = rightNode;
            }

            // 有匹配的Node和Event
            if (chainPath != null) {
                // 生成Event的查询信息
                RdbQueryInfo eventQueryInfo = makeQueryInfoByEvent(
                        eventDefinition,
                        eventRdbTable,
                        eventLabel,
                        chainPath,
                        queryInfo
                );
                // 加入查询集合
                this.queryInfoList.add(eventQueryInfo);


                // 生成下一级Node的查询信息
                RdbQueryInfo nextNodeQueryInfo = makeQueryInfo(nextNode, chainData);

                // 设置关联
                RdbJoinInfo joinInfo = new RdbJoinInfo();
                joinInfo.setJoinAlias(eventQueryInfo.getAlias());
                joinInfo.setJoinType(RdbJoinInfo.TYPE_INNER);
                if (ChainPath.LEFT.equals(chainPath)) {
                    // 作为关联右Node
                    joinInfo.putJoinColumn(nextNodeQueryInfo.getIdAlias(), eventQueryInfo.getRightAlias());
                } else {
                    // 作为关联左Node
                    joinInfo.putJoinColumn(nextNodeQueryInfo.getIdAlias(), eventQueryInfo.getLeftAlias());
                }
                nextNodeQueryInfo.setJoin(joinInfo);

                // 设置过滤
                RdbExistsInfo existsInfo = new RdbExistsInfo();
                // 设置被驱动表
                existsInfo.setSelfTable(nextNodeQueryInfo.getTable());
                // 设置驱动表
                existsInfo.setExistsTable(eventQueryInfo.getTable());
                if (ChainPath.LEFT.equals(chainPath)) {
                    // 作为过滤右Node
                    existsInfo.putExistsColumn(
                            nextNodeQueryInfo.getColumns().get(nextNodeQueryInfo.getIdAlias()),
                            eventQueryInfo.getColumns().get(eventQueryInfo.getRightAlias())
                    );
                } else {
                    // 作为过滤左Node
                    existsInfo.putExistsColumn(
                            nextNodeQueryInfo.getColumns().get(nextNodeQueryInfo.getIdAlias()),
                            eventQueryInfo.getColumns().get(eventQueryInfo.getLeftAlias())
                    );
                }
                existsInfo.setParams(nextNodeQueryInfo.getParams());
                eventQueryInfo.addExists(existsInfo);
                // 如果nextNode有exists条件，加入到Event和nextNode的Exists中
                if (nextNodeQueryInfo.hasExists()) {
                    existsInfo.getExistsList().add(nextNodeQueryInfo.getExists());
                }
            }
        });
        return queryInfo;
    }

    /**
     * 基于Node生成{@link RdbQueryInfo}
     *
     * @param nodeDefinition Node定义
     * @param nodeRdbTable   Node和Rdb表映射
     * @param nodeLabel      Node标签
     * @return Rdb查询信息
     */
    private RdbQueryInfo makeQueryInfoByNode(
            final SdNodeDefinition nodeDefinition,
            final RelationShipRdbTable nodeRdbTable,
            final String nodeLabel
    ) {
        // 将Node定义加入返回结果
        queryResult.getUxdf()
                .getSd()
                .getNode()
                .getImpl()
                .put(nodeDefinition.getNodeName(), nodeDefinition);

        String startTableName = nodeRdbTable.getName();
        RdbQueryInfo queryInfo = new RdbQueryInfo();
        // Node定义
        queryInfo.setNodeDefinition(nodeDefinition);
        // 是否Node
        queryInfo.setNodeEntity(true);
        // 表名
        queryInfo.setTable(startTableName);
        // 表别名
        queryInfo.setAlias(makeTableAlias());
        // 标签
        queryInfo.setLabel(nodeLabel);
        // 填充查询信息列名
        this.fillColumn(
                queryInfo,
                nodeDefinition.getProp(),
                nodeRdbTable
        );
        // 填充查询信息查询参数
        this.fillQueryParam(
                queryInfo,
                nodeLabel
        );
        return queryInfo;
    }

    /**
     * 基于Event生成{@link RdbQueryInfo}
     *
     * @param eventDefinition Event定义
     * @param eventRdbTable   Event和Rdb表映射
     * @param eventLabel      Event标签
     * @param chainPath       Event所处关系链方向
     * @param joinQueryInfo   要关联的查询信息
     * @return 查询信息
     */
    private RdbQueryInfo makeQueryInfoByEvent(
            final SdEventDefinition eventDefinition,
            final RelationShipRdbTable eventRdbTable,
            final String eventLabel,
            final ChainPath chainPath,
            final RdbQueryInfo joinQueryInfo
    ) {
        // 将Event定义加入返回结果
        queryResult.getUxdf()
                .getSd()
                .getEvent()
                .put(eventDefinition, true);
        RdbQueryInfo queryInfo = new RdbQueryInfo();
        queryInfo.setEventDefinition(eventDefinition);
        queryInfo.setEventEntity(true);
        // 表名
        queryInfo.setTable(eventRdbTable.getName());
        // 表别名
        queryInfo.setAlias(makeTableAlias());
        // 标签
        queryInfo.setLabel(eventLabel);
        // 填充查询信息列名
        this.fillColumn(
                queryInfo,
                eventDefinition.getProp(),
                eventRdbTable
        );
        // 填充查询信息查询参数
        this.fillQueryParam(
                queryInfo,
                eventLabel
        );
        // 设置关联
        RdbJoinInfo joinInfo = new RdbJoinInfo();
        joinInfo.setJoinAlias(joinQueryInfo.getAlias());
        joinInfo.setJoinType(RdbJoinInfo.TYPE_INNER);
        if (ChainPath.LEFT.equals(chainPath)) {
            // 关联左Node
            joinInfo.putJoinColumn(queryInfo.getLeftAlias(), joinQueryInfo.getIdAlias());
        } else {
            // 关联右Node
            joinInfo.putJoinColumn(queryInfo.getRightAlias(), joinQueryInfo.getIdAlias());
        }
        queryInfo.setJoin(joinInfo);

        // 设置过滤条件
        RdbExistsInfo existsInfo = new RdbExistsInfo();
        // 设置被驱动表
        existsInfo.setSelfTable(queryInfo.getTable());
        // 设置作为条件的驱动表
        existsInfo.setExistsTable(joinQueryInfo.getTable());
        if (ChainPath.LEFT.equals(chainPath)) {
            // 过滤左Node
            existsInfo.putExistsColumn(
                    queryInfo.getColumns().get(queryInfo.getLeftAlias()),
                    joinQueryInfo.getColumns().get(joinQueryInfo.getIdAlias())
            );
        } else {
            // 过滤右Node
            existsInfo.putExistsColumn(
                    queryInfo.getColumns().get(queryInfo.getRightAlias()),
                    joinQueryInfo.getColumns().get(joinQueryInfo.getIdAlias())
            );
        }
        existsInfo.setParams(queryInfo.getParams());
        joinQueryInfo.addExists(existsInfo);
        queryInfo.setExists(existsInfo);
        return queryInfo;
    }

    /**
     * 检查结束Node在当前Event下是否是起始Node的下一个
     *
     * @param startNode   起始Node
     * @param endNode     结束Node
     * @param eventEntity Event
     * @param eventChain  关系链
     * @return 是否是下一个
     */
    private boolean checkIsNext(
            final NodeEntity startNode,
            final NodeEntity endNode,
            final EventEntity eventEntity,
            final String eventChain
    ) {
        if (!endNode.equals(startNode) && !endNode.containsKey(eventChain)) {
            // 设置被当前关系链使用过
            endNode.put(eventChain, true);
            // 设置被当前关系链使用过
            eventEntity.put(eventChain, true);
            // 设置被当前关系链使用过
            startNode.put(eventChain, true);

            return true;
        }
        return false;
    }

    /**
     * 填充查询信息{@link RdbQueryInfo}的列信息
     *
     * @param queryInfo           查询信息
     * @param propertyDefinitions 属性定义
     * @param rdbTable            Node对应表映射
     */
    private void fillColumn(
            final RdbQueryInfo queryInfo,
            final Map<String, SdProperty> propertyDefinitions,
            final RelationShipRdbTable rdbTable
    ) {
        // 遍历所有列信息
        rdbTable.getColumn().forEach((propName, columnName) -> {
            // 二进制列不返回
            SdProperty property = propertyDefinitions.get(propName);
            if (property != null && property.getBase().equals(SdBaseType.Binary)) {
//                columnAlias += SUFFIX_BINARY;
                return;
            }
            // 生成列别名
            String columnAlias = this.makeColumnAlias();
            switch (propName) {
                // 记录ID别名，用于关联
                case SdEntity.ATTR_ID:
                    queryInfo.setIdAlias(columnAlias);
                    break;
                // 记录ID别名，用于关联
                case EventEntity.ATTR_LEFT:
                    queryInfo.setLeftAlias(columnAlias);
                    break;
                // 记录ID别名，用于关联
                case EventEntity.ATTR_RIGHT:
                    queryInfo.setRightAlias(columnAlias);
                    break;
            }
            // 将列别名加入查询信息
            queryInfo.getColumns().put(columnAlias, columnName);
        });
    }

    /**
     * 填充查询信息{@link RdbQueryInfo}参数{@link SdDataQueryParam}
     *
     * @param queryInfo 查询信息
     * @param label     查询表标签
     */
    private void fillQueryParam(
            final RdbQueryInfo queryInfo,
            final String label
    ) {
        // 获取所有查询参数
        List<SdDataQueryParam> queryParams = this.queryRequest.getParams().get(label);
        // 未设置参数
        if (queryParams == null || queryParams.isEmpty()) {
            return;
        }

        // 查询信息查询的是Node
        if (queryInfo.isNodeEntity()) {
            queryParams = uxdfRdbConvert.convertNodeQueryParam(
                    queryInfo.getNodeDefinition().getNodeName(),
                    queryParams
            );
        }
        // 查询信息查询的是Event
        if (queryInfo.isEventEntity()) {
            queryParams = uxdfRdbConvert.convertEventQueryParam(
                    queryInfo.getEventDefinition().getEventName(),
                    queryInfo.getEventDefinition().getLeftNodeName(),
                    queryInfo.getEventDefinition().getRightNodeName(),
                    queryParams
            );
        }
        if (queryParams != null && !queryParams.isEmpty()) {
            queryParams.forEach(queryParam -> {
                // 转换参数
                queryInfo.getParams().add(new RdbQueryParam(
                        queryParam.getProperty(),
                        queryParam.getValue(),
                        queryParam.getLogic()
                ));
            });
        }

    }

    /**
     * 生成表别名
     *
     * @return 表别名
     */
    private String makeTableAlias() {
        return "T_" + this.tableSeq.getAndIncrement();
    }

    /**
     * 生成列别名
     *
     * @return 列别名
     */
    private String makeColumnAlias() {
        return "C_" + this.columnSeq.getAndIncrement();
    }

    /**
     * 将当前关系串转换为{@link info.ralab.uxdf.instance.SdEntity}填充到{@link SdData}中。
     *
     * @param chain      填充得源
     * @param sdData     填充的目标
     * @param labelIdMap Chain中label和SdData中id得映射关系
     */
    private void fillChainToSdData(
            final UXDFChain chain,
            final SdData sdData,
            final Map<String, String> labelIdMap
    ) {
        chain.forEach(uxdfChainItems -> {
            // 遍历关系链中每一项
            uxdfChainItems.forEach(chainItem -> {
                // Node需要根据标签保持唯一性
                QueryNodeEntity leftNode = new QueryNodeEntity();
                String leftNodeLabel = chainItem.getLeftNodeLabel();
                String leftNodeId = labelIdMap.computeIfAbsent(leftNodeLabel, key -> IdMaker.temp());
                leftNode.id(leftNodeId).sd(leftNodeLabel);

                // Node需要根据标签保持唯一性
                QueryNodeEntity rightNode = new QueryNodeEntity();
                String rightNodeLabel = chainItem.getRightNodeLabel();
                String rightNodeId = labelIdMap.computeIfAbsent(rightNodeLabel, key -> IdMaker.temp());
                rightNode.id(rightNodeId).sd(rightNodeLabel);

                // Event不需要保持唯一性
                QueryEventEntity event = new QueryEventEntity();
                event.id(IdMaker.temp())
                        .sd(chainItem.getEventLabel())
                        .leftNode(leftNode)
                        .rightNode(rightNode);

                switch (chainItem.getChainPath()) {
                    case LEFT:
                        sdData.addNodeIfAbsent(leftNode);
                        sdData.addNodeIfAbsent(rightNode);
                        break;
                    case RIGHT:
                        sdData.addNodeIfAbsent(rightNode);
                        sdData.addNodeIfAbsent(leftNode);
                        break;
                }
                sdData.computeEvent(event, existEvent -> event);
            });
        });
    }
}
