package info.ralab.uxdf.rdb.mapper;

import com.alibaba.fastjson.JSONObject;
import info.ralab.uxdf.model.SdDataQueryOrder;
import info.ralab.uxdf.model.SdDataQueryPage;
import info.ralab.uxdf.rdb.DataAuth;
import info.ralab.uxdf.rdb.model.RdbQueryInfo;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Collection;
import java.util.List;

@Mapper
public interface UXDFQueryMapper {


    List<JSONObject> query(
            @Param("queryInfoList") Collection<RdbQueryInfo> queryInfoList,
            @Param("queryOrderList") List<SdDataQueryOrder> orders,
            @Param("queryPage") SdDataQueryPage page,
            @Param("auth") DataAuth dataAuth
    );

    long count(
            @Param("queryInfo") RdbQueryInfo queryInfo
    );
}
