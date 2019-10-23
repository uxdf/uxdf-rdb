package info.ralab.uxdf.rdb.utils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import info.ralab.uxdf.UXDFAuthException;
import info.ralab.uxdf.UXDFException;
import info.ralab.uxdf.UXDFLoader;
import info.ralab.uxdf.definition.*;
import info.ralab.uxdf.instance.EventEntity;
import info.ralab.uxdf.instance.IdMaker;
import info.ralab.uxdf.instance.NodeEntity;
import info.ralab.uxdf.instance.SdEntity;
import info.ralab.uxdf.model.SdDataQueryParam;
import info.ralab.uxdf.model.SdDataQueryRequest;
import info.ralab.uxdf.model.SdDataQueryResult;
import info.ralab.uxdf.rdb.convert.OracleTimestampConvert;
import info.ralab.uxdf.rdb.exception.UXDFSaveErrorType;
import info.ralab.uxdf.rdb.exception.UXDFSaveException;
import info.ralab.uxdf.service.StorageService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.MessageSource;

import java.util.*;

/**
 * UXDF数据校验
 */
@Slf4j
public class UXDFRdbValidator {

    private StorageService storageService;
    private MessageSource messageSource;

    private OracleTimestampConvert oracleTimestampConvert = new OracleTimestampConvert();
    /**
     * 拒绝更新操作的Sd
     */
    @Getter
    private List<String> denyUpdateSd = Lists.newArrayList();

    public UXDFRdbValidator(
            final StorageService storageService,
            final MessageSource messageSource
    ) {
        this.storageService = storageService;
        this.messageSource = messageSource;
    }


    /**
     * 根据Node的操作类型进行校验
     *
     * @param node Node名称
     */
    public void check(final NodeEntity node) throws UXDFException {
        if (node == null) {
            throw new UXDFException("Node实体为空。");
        }

        SdOperateType operate = node.getOperate();
        if (operate == null) { // 没有操作,不进行检查
            return;
        }

        final String nodeName = node.get__Sd();
        if (nodeName == null) {
            throw new UXDFException("Node实体定义为空。");
        }

        final SdNodeDefinition nodeImpl = UXDFLoader.getNode(nodeName);
        if (nodeImpl == null) {
            throw new UXDFException(String.format("Node实体定义[%s]不存在。", nodeName));
        }
        final String nodeTitle = nodeImpl.getTitle();

        final String id = node.get__Id();
        if (id == null) {
            throw new UXDFException(String.format("[%s]逻辑ID为空。", nodeTitle));
        }

        // 检查操作是否允许
        if (isNodeOperateDeny(operate, nodeName)) {
            throw new UXDFAuthException(String.format("[%s]禁止 %s 操作。", nodeTitle, operate));
        }

        final Map<String, SdProperty> nodeAttr = UXDFLoader.getBaseUXDF().getSd().getNode().getAttr();
        final Map<String, SdProperty> nodeProp = nodeImpl.getProp();
        final Map<String, SdProperty> nodeAllProps = Maps.newHashMap(nodeAttr);
        nodeAllProps.putAll(nodeProp);
        String[] uniqueIndexArray = nodeImpl.getUniqueIndex();

        switch (operate) {
            case create:
                for (String key : nodeAttr.keySet()) {
                    SdProperty prop = nodeAttr.get(key);

                    final Object value = node.containsKey(key) ? node.get(key) : prop.getDefaultValue();

                    this.checkValue(nodeTitle, key, value, prop);
                }

                for (String key : nodeAllProps.keySet()) {
                    SdProperty prop = nodeAllProps.get(key);

                    final Object value = node.containsKey(key) ? node.get(key) : prop.getDefaultValue();

                    this.checkValue(nodeTitle, key, value, prop);
                }

                // 唯一性约束检查
                if (uniqueIndexArray != null && uniqueIndexArray.length != 0) {
                    // 判断数据库中是否已经存在相同记录
                    SdDataQueryRequest queryRequest = new SdDataQueryRequest();
                    queryRequest.getChains().add(nodeName);
                    queryRequest.getParams().put(nodeName, Lists.newArrayList(
                            SdDataQueryParam.equal(NodeEntity.ATTR_UUID, node.get__Uuid())
                    ));

                    SdDataQueryResult queryResult = this.storageService.queryData(queryRequest);
                    if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                        String[] displayArray = nodeImpl.getDisplay();
                        StringBuilder display = new StringBuilder();
                        if (displayArray != null && displayArray.length > 0) {
                            for (String displayProp : displayArray) {
                                if (node.containsKey(displayProp)) {
                                    display.append(node.get(displayProp));
                                }
                            }
                        }
                        throw new UXDFSaveException(messageSource.getMessage(
                                "error.save.unique.node",
                                new Object[]{nodeImpl.getTitle(), display.toString()},
                                Locale.getDefault()
                        ), SdOperateType.create, UXDFSaveErrorType.unique);
                    }
                }
                break;
            case update:
                if (!IdMaker.effective(id)) {
                    throw new UXDFException("更新操作的实体逻辑ID不是有效ID。");
                }
                // 获取原有记录
                SdDataQueryResult queryResultById = this.storageService.getDataById(
                        nodeName,
                        id
                );
                if (queryResultById.getUxdf().getData().isNodeEmpty()) {
                    throw new UXDFException(String.format("[%s|%s]不存在。", nodeTitle, id));
                }

                for (String key : nodeAllProps.keySet()) {
                    SdProperty prop = nodeAllProps.get(key);

                    final Object value = node.containsKey(key) ? node.get(key) : prop.getDefaultValue();
                    // 更新操作只检查非空值
                    if (value != null) {
                        this.checkValue(nodeTitle, key, value, prop);
                    }
                }

                // 唯一性约束检查
                if (uniqueIndexArray != null && uniqueIndexArray.length != 0) {
                    // 判断数据库中是否已经存在相同记录
                    SdDataQueryRequest queryRequestByUuid = new SdDataQueryRequest();
                    queryRequestByUuid.getChains().add(nodeName);
                    queryRequestByUuid.getParams().put(nodeName, Lists.newArrayList(
                            SdDataQueryParam.equal(NodeEntity.ATTR_UUID, node.get__Uuid()),
                            // 更新的时候，排除当前记录
                            SdDataQueryParam.notEqual(NodeEntity.ATTR_ID, node.get__Id())
                    ));

                    SdDataQueryResult queryResult = this.storageService.queryData(queryRequestByUuid);
                    if (!queryResult.getUxdf().getData().isNodeEmpty()) {
                        throw new UXDFException(String.format("[%s]违反唯一约束。", nodeTitle));
                    }
                }
                break;
            case delete:
                if (!IdMaker.effective(id)) {
                    throw new UXDFException("删除操作的实体逻辑ID不是有效ID。");
                }
                break;
            default:
                for (String key : nodeAttr.keySet()) {
                    SdProperty prop = nodeAttr.get(key);

                    final Object value = node.containsKey(key) ? node.get(key) : prop.getDefaultValue();

                    this.checkValue(nodeTitle, key, value, prop);
                }

                for (String key : nodeAllProps.keySet()) {
                    SdProperty prop = nodeAllProps.get(key);

                    final Object value = node.containsKey(key) ? node.get(key) : prop.getDefaultValue();

                    this.checkValue(nodeTitle, key, value, prop);
                }
        }

    }

    /**
     * 检查Event
     *
     * @param event Event实例
     */
    public void check(final EventEntity event) {
        if (event == null) {
            throw new UXDFException("event is null.");
        }

        SdOperateType operate = event.getOperate();
        if (operate == null) { // 没有操作,不进行检查
            return;
        }

        // 检查Event定义是否正确
        Map<String, Map<String, SdEventDefinition>> leftEvent = UXDFLoader.getEvent(event.get__Sd());
        if (leftEvent == null) {
            throw new UXDFException(String.format("event [*-%s>*] not defined.", event.get__Sd()));
        }
        Map<String, SdEventDefinition> rightEvent = leftEvent.get(event.get__LeftSd());
        if (rightEvent == null) {
            throw new UXDFException(String.format("event [%s-%s>*] not defined.", event.get__LeftSd(), event.get__Sd()));
        }
        SdEventDefinition eventDefinition = rightEvent.get(event.get__RightSd());
        if (eventDefinition == null) {
            throw new UXDFException(String.format("event [%s-%s>%s] not defined.", event.get__LeftSd(), event.get__Sd(), event.get__RightSd()));
        }

        // 操作是否被拒绝
        if (isEventOperateDeny(operate, event.get__LeftSd(), event.get__Sd(), event.get__RightSd())) {
            throw new UXDFException(String.format("event [%s-%s>%s] %s was denied.", event.get__LeftSd(), event.get__Sd(), event.get__RightSd(), operate.toString()));
        }

        // TODO 唯一性约束检查
    }

    /**
     * 操作是否被拒绝
     *
     * @param operate  操作
     * @param nodeName Node名称
     * @return 是否拒绝
     */
    private boolean isNodeOperateDeny(final SdOperateType operate, final String nodeName) {
        // TODO 需要通过数据权限判断
        return false;
    }

    /**
     * 操作是否被拒绝
     *
     * @param operate       操作
     * @param leftNodeName  左Node名称
     * @param eventName     Event名称
     * @param rightNodeName 右Node名称
     * @return 是否拒绝
     */
    private boolean isEventOperateDeny(
            final SdOperateType operate,
            final String leftNodeName,
            final String eventName,
            final String rightNodeName
    ) {
        // TODO 需要通过数据权限判断
        return false;
    }

    /**
     * 检查值是否符合要求
     *
     * @param nodeTitle     Sd标题
     * @param propertyName  属性名称
     * @param propertyValue 属性值
     * @param property      属性定义
     */
    private void checkValue(
            final String nodeTitle,
            final String propertyName,
            final Object propertyValue,
            final SdProperty property
    ) {
        final String propertyTitle = property.getTitle();
        log.debug("{} check prop[{}], value[{}], prop:[{}]", nodeTitle, propertyTitle, propertyValue, property);
        // 必填检查
        if (property.isRequired() && propertyValue == null) {
            throw new UXDFException(String.format("[%s]的属性[%s]不能为空。", nodeTitle, propertyTitle));
        } else if (propertyValue == null) { // 空值不再进行检查
            return;
        }
        final SdBaseType type = property.getBase();

        // 类型检查，以及数据上限和下限
        switch (type) {
            case Integer:
                final Long integerValue = SdEntity.getBaseInteger(propertyValue);
                final Long integerLower = SdEntity.getBaseInteger(property.getLowerLimit());
                final Long integerUpper = SdEntity.getBaseInteger(property.getUpperLimit());
                if (integerValue == null) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]类型错误。期望[%s]，实际[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    SdBaseType.Integer,
                                    propertyValue.getClass().getName()
                            )
                    );
                }
                if (integerLower != null && integerValue < integerLower) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]值小于最小限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    integerLower
                            )
                    );
                }
                if (integerUpper != null && integerValue > integerUpper) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]大于最大限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    integerUpper
                            )
                    );
                }
                break;
            case Float:
                final Double floatValue = SdEntity.getBaseFloat(propertyValue);
                final Double floatLower = SdEntity.getBaseFloat(property.getLowerLimit());
                final Double floatUpper = SdEntity.getBaseFloat(property.getUpperLimit());
                if (floatValue == null) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]类型错误。期望[%s]，实际[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    SdBaseType.Float,
                                    propertyValue.getClass().getName()
                            )
                    );
                }
                if (floatLower != null && floatValue < floatLower) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]值小于最小限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    floatLower
                            )
                    );
                }
                if (floatUpper != null && floatValue > floatUpper) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]大于最大限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    floatUpper
                            )
                    );
                }
                break;
            case Boolean:
                final Boolean booleanValue = SdEntity.getBaseBoolean(propertyValue);
                if (booleanValue == null) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]类型错误。期望[%s]，实际[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    SdBaseType.Boolean,
                                    propertyValue.getClass().getName()
                            )
                    );
                }
                break;
            case Datetime:
                final Date dateValue = SdEntity.getBaseDate(propertyValue, oracleTimestampConvert);
                final Date dateLower = SdEntity.getBaseDate(property.getLowerLimit(), oracleTimestampConvert);
                final Date dateUpper = SdEntity.getBaseDate(property.getUpperLimit(), oracleTimestampConvert);
                if (dateValue == null) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]类型错误。期望[%s]，实际[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    SdBaseType.Datetime,
                                    propertyValue.getClass().getName()
                            )
                    );
                }
                if (dateLower != null && dateValue.compareTo(dateLower) < 0) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]值小于最小限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    dateLower
                            )
                    );
                }
                if (dateUpper != null && dateValue.compareTo(dateUpper) > 0) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]大于最大限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    dateUpper
                            )
                    );
                }
                break;
            case String:
            case Binary:
                final String stringValue = SdEntity.getBaseString(propertyValue);
                final Long stringLower = SdEntity.getBaseInteger(property.getLowerLimit());
                final Long stringUpper = SdEntity.getBaseInteger(property.getUpperLimit());
                // 如果不限制长度直接跳过
                if (stringUpper != null && stringUpper == -1L) {
                    break;
                }
                if (stringValue == null) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]类型错误。期望[%s]，实际[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    SdBaseType.String,
                                    propertyValue.getClass().getName()
                            )
                    );
                }
                if (stringLower != null && stringValue.length() < stringLower) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]大小小于最小限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    stringLower
                            )
                    );
                }
                if (stringUpper != null && stringValue.length() > stringUpper) {
                    throw new UXDFException(
                            String.format(
                                    "[%s]的属性[%s]大小大于最大限制[%s]",
                                    nodeTitle,
                                    propertyTitle,
                                    stringUpper
                            )
                    );
                }
                break;
        }

        // 检查规则校验
        if (property.getValidRule() != null && property.getValidRule().length > 0) {
            SdPropertyValidRule[][] orRules = property.getValidRule();
            // 汇总错误信息
            List<String> errorMessages = new ArrayList<>();
            // 遍历校验规则
            boolean orResult = false;
            for (SdPropertyValidRule[] andRules : orRules) {
                boolean andResult = true;

                for (SdPropertyValidRule andRule : andRules) {
                    andResult = andRule.check(type, propertyValue);
                    if (!andResult) {
                        errorMessages.add(andRule.getMessage());
                        break;
                    }

                }
                orResult = andResult;
                if (orResult) {
                    break;
                }
            }
            if (!orResult) {
                throw new UXDFException(
                        String.format(
                                "[%s]的属性[%s]不符合[%s]。",
                                nodeTitle,
                                propertyTitle,
                                Joiner.on("\n").join(errorMessages)
                        )
                );
            }

        }
    }
}
