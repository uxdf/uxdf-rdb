package info.ralab.uxdf.rdb.mapper;

import com.alibaba.fastjson.JSONObject;
import info.ralab.uxdf.model.SdDataQueryOrder;
import info.ralab.uxdf.model.SdDataQueryPage;
import info.ralab.uxdf.model.SdDataQueryParam;
import info.ralab.uxdf.rdb.DataAuth;
import info.ralab.uxdf.rdb.model.*;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.io.InputStream;
import java.sql.Blob;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Mapper
public interface UXDFMapper {

    /**
     * 根据ID获得SD
     *
     * @param tableName
     * @param id
     * @return
     */
    JSONObject get(
            @Param("table") String tableName,
            @Param("id") Long id,
            @Param("repository") String repository,
            @Param("branch") String branch,
            @Param("version") String version,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 根据ID获得SD
     *
     * @param tableName
     * @param ids
     * @param repository
     * @param branch
     * @return
     */
    List<JSONObject> get(
            @Param("table") String tableName,
            @Param("id") Collection<Long> ids,
            @Param("repository") String repository,
            @Param("branch") String branch,
            @Param("version") String version,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 根据SD和参数集合查询符合结果的SD
     *
     * @param tableName
     * @param params
     * @param orders
     * @param updateLock
     * @return
     */
    List<JSONObject> query(
            @Param("table") String tableName,
            @Param("params") List<SdDataQueryParam> params,
            @Param("orders") List<SdDataQueryOrder> orders,
            @Param("auth") DataAuth dataAuth,
            @Param("updateLock") boolean updateLock);

    /**
     * 根据Node和参数集合查询符合结果的Node
     *
     * @param nodeName
     * @param params
     * @return
     */
    long queryCount(
            @Param("table") String nodeName,
            @Param("params") List<SdDataQueryParam> params,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 根据Node和参数集合查询符合结果的Node
     *
     * @param nodeName
     * @param params
     * @param page
     * @param orders
     * @return
     */
    List<JSONObject> queryPage(
            @Param("table") String nodeName,
            @Param("params") List<SdDataQueryParam> params,
            @Param("page") SdDataQueryPage page,
            @Param("orders") List<SdDataQueryOrder> orders,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 新增Node
     *
     * @param insertParam
     */
    int insert(InsertParam insertParam);

    /**
     * 保存Event
     *
     * @param insertParam
     * @return
     */
    int insertEvent(InsertParam insertParam);

    /**
     * 更新Node
     *
     * @param updateParam
     * @return
     */
    int update(UpdateParam updateParam);

    /**
     * 根据左Sd类型，和ID。删除Event
     *
     * @param eventTable
     * @param leftId
     * @return
     */
    int deleteEventByLeftNode(
            @Param("table") String eventTable,
            @Param("left") String leftId,
            @Param("leftSd") String leftSd
    );

    /**
     * 根据右Sd类型，和ID。删除Event
     *
     * @param eventTable
     * @param rightId
     * @return
     */
    int deleteEventByRightNode(
            @Param("table") String eventTable,
            @Param("right") String rightId,
            @Param("rightSd") String rightSd
    );

    /**
     * 根据ID删除Event
     *
     * @param eventTable
     * @param eventId
     * @param dataAuth
     * @return
     */
    int deleteEvent(
            @Param("table") String eventTable,
            @Param("id") String eventId,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 根据ID删除Node
     *
     * @param nodeTable
     * @param nodeId
     * @return
     */
    int deleteNode(
            @Param("table") String nodeTable,
            @Param("id") String nodeId,
            @Param("auth") DataAuth dataAuth
    );

    /**
     * 移除表
     *
     * @param table
     */
    void dropTable(@Param("table") String table);

    /**
     * 根据Tower code 获取Tower及相关Line
     *
     * @param towerTable
     * @param towerAlias
     * @param lineTable
     * @param lineAlias
     * @param belongToTable
     * @param belongToAlias
     * @param code
     * @param orders
     * @return
     */
    List<JSONObject> getTowerByCode(
            @Param("towerTable") String towerTable,
            @Param("towerAlias") Map<String, String> towerAlias,
            @Param("lineTable") String lineTable,
            @Param("lineAlias") Map<String, String> lineAlias,
            @Param("belongToTable") String belongToTable,
            @Param("belongToAlias") Map<String, String> belongToAlias,
            @Param("code") String code,
            @Param("repository") String repository,
            @Param("branch") String branch,
            @Param("orders") List<SdDataQueryOrder> orders
    );

    List<JSONObject> getTowerByCodePage(
            @Param("towerTable") String towerTable,
            @Param("towerAlias") Map<String, String> towerAlias,
            @Param("lineTable") String lineTable,
            @Param("lineAlias") Map<String, String> lineAlias,
            @Param("belongToTable") String belongToTable,
            @Param("belongToAlias") Map<String, String> belongToAlias,
            @Param("code") String code,
            @Param("repository") String repository,
            @Param("branch") String branch,
            @Param("orders") List<SdDataQueryOrder> orders,
            @Param("page") SdDataQueryPage page
    );

    long getTowerByCodeCount(
            @Param("towerTable") String towerTable,
            @Param("towerAlias") Map<String, String> towerAlias,
            @Param("lineTable") String lineTable,
            @Param("lineAlias") Map<String, String> lineAlias,
            @Param("belongToTable") String belongToTable,
            @Param("belongToAlias") Map<String, String> belongToAlias,
            @Param("code") String code,
            @Param("repository") String repository,
            @Param("branch") String branch,
            @Param("orders") List<SdDataQueryOrder> orders
    );

    void createTempIdTable(
            @Param("tableName") String tableName
    );

    int insertTempId(
            @Param("tableName") String tableName,
            @Param("ids") List<Long> ids
    );

    int insertBinary(final UXDFBinaryFileInfo fileInfo);

    InputStream getBinary(
            @Param("table") final String table,
            @Param("column") final String column,
            @Param("uuid") final String uuid
    );

    /**
     * 检查表是否存在
     *
     * @param table
     * @return
     */
    String checkTableExists(@Param("table") String table);

    /**
     * 设置环境
     */
    void setEnv();

    void createIdArea();

    /**
     * 获取ID分区
     *
     * @return ID分区
     */
    String getIdArea();

    void addIdArea(@Param("area") String area);

    /**
     * 设置ID分区
     *
     * @param area 分区
     */
    void setIdArea(@Param("area") String area);

    /**
     * 删除指定左右ID的Event
     *
     * @param table Event对应的表
     * @param left  左节点ID
     * @param right 右节点ID
     * @return 删除的数量
     */
    int deleteEventByLeftAndRight(
            @Param("table") String table,
            @Param("left") String left,
            @Param("right") String right
    );

    /**
     * 删除指定Node全部数据
     *
     * @param table 表名
     * @return 删除的数量
     */
    int deleteNodeAll(@Param("table") String table);
}
