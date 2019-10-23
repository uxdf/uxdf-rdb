package info.ralab.uxdf.rdb;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import info.ralab.uxdf.SdData;
import info.ralab.uxdf.definition.SdOperateType;
import info.ralab.uxdf.instance.EventEntity;
import info.ralab.uxdf.instance.IdMaker;
import info.ralab.uxdf.instance.NodeEntity;
import info.ralab.uxdf.instance.RdbIdAreaMaker;
import info.ralab.uxdf.model.*;
import info.ralab.uxdf.service.StorageService;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@Slf4j
public class RdbStorageServiceTest {

    @Autowired
    private StorageService storageService;
    @Autowired
    private RdbIdAreaMaker rdbIdAreaMaker;

    @Test
    public void testRdbInit() {
        storageService.init();
    }

    @Test
    @Transactional
    @Rollback()
    public void testSave() {
        storageService.init();

        NodeEntity userNode = saveUser();

        saveUserGroupHaveUser(userNode);
    }


    @Test
    @Transactional
    @Rollback()
    public void testQuery() {
        storageService.init();

        // 设置ID生成器的分区
        IdMaker.init(rdbIdAreaMaker);

        NodeEntity userNode = saveUser();

        saveUserGroupHaveUser(userNode);

        int queryNum = 5;
        while (queryNum > 0) {
            SdDataQueryRequest queryRequest = new SdDataQueryRequest();
            queryRequest.getChains().add("U:User<HAVE-UserGroup");

            queryRequest.getParams().computeIfAbsent("U", key -> Lists.newArrayList(
                    SdDataQueryParam.equal("nickname", userNode.get("nickname")),
                    SdDataQueryParam.startWith(
                            "username",
                            userNode.getString("username").substring(0, 3)
                    )
            ));

            queryRequest.setMain(new SdDataQueryRequest.PageOrder());
            queryRequest.getMain().setAlias("U");
            queryRequest.getMain().setPage(new SdDataQueryPage());
            queryRequest.getMain().setOrders(Lists.newArrayList(
                    SdDataQueryOrder.asc("nickname"),
                    SdDataQueryOrder.desc("username")
            ));


            SdDataQueryResult queryResult = storageService.queryData(queryRequest);

            System.out.println(JSON.toJSONString(queryResult));
            queryResult.getUxdf().getData().getUnmodifiableNode().forEach(nodeEntity -> {
                if (nodeEntity.get__Sd().equals("User")) {

                    UXDFBinaryFileInfo photoFileInfo = this.storageService.getUXDFBinaryFile(
                            nodeEntity.get__Sd(), "photo", nodeEntity.getUUID()
                    );

                    try (InputStream inputStream = photoFileInfo.getInputStream()) {
                        byte[] values = new byte[(int)photoFileInfo.getLength()];
                        Assert.assertNotEquals(-1, inputStream.read(values));
                    } catch (IOException e) {
                        log.error(e.getLocalizedMessage(), e);
                        Assert.assertNull(e);
                    }
                }
            });
            queryNum--;
        }
    }

    private void queryUserGroupHaveUserHaveRole(final NodeEntity userNode) {
        SdDataQueryRequest queryRequest = new SdDataQueryRequest();
        queryRequest.getChains().add("Role<HAVE-User<HAVE-UserGroup");

        queryRequest.getParams().computeIfAbsent("User", key -> Lists.newArrayList(
                SdDataQueryParam.equal("nickname", userNode.get("nickname")),
                SdDataQueryParam.startWith(
                        "username",
                        userNode.getString("username").substring(0, 3)
                )
        ));

        queryRequest.setMain(new SdDataQueryRequest.PageOrder());
        queryRequest.getMain().setAlias("User");
        queryRequest.getMain().setPage(new SdDataQueryPage());
        queryRequest.getMain().setOrders(Lists.newArrayList(
                SdDataQueryOrder.asc("nickname"),
                SdDataQueryOrder.desc("username")
        ));


        SdDataQueryResult queryResult = storageService.queryData(queryRequest);

        System.out.println(JSON.toJSONString(queryResult));
    }

    private void saveUserGroupHaveUser(NodeEntity userNode) {
        SdData saveData;
        SdDataSaveResult saveResult;

        NodeEntity userGroupNode = new NodeEntity()
                .id(IdMaker.next())
                .sd("UserGroup");
        userGroupNode.put("name", String.valueOf(System.currentTimeMillis()));
        userGroupNode.setOperate(SdOperateType.create);

        EventEntity haveEvent = new EventEntity()
                .id(IdMaker.next())
                .sd("HAVE")
                .leftNode(userGroupNode)
                .rightNode(userNode);
        haveEvent.setOperate(SdOperateType.create);

        saveData = new SdData();
        saveData.addNodeIfAbsent(userGroupNode);
        saveData.addEventIfAbsent(haveEvent);
        saveResult = this.storageService.saveData(saveData, null, null, false);

        System.out.println(JSON.toJSONString(saveResult));
        System.out.println(JSON.toJSONString(saveData));
        Assert.assertTrue(saveResult.getNodeCreateNum().get() > 0);
        Assert.assertTrue(IdMaker.effective(saveData.getUnmodifiableEvent("HAVE").get(0).get__Id()));
    }

    private NodeEntity saveUser() {
        NodeEntity userNode = new NodeEntity()
                .id(IdMaker.next())
                .sd("User");
        userNode.put("nickname", String.valueOf(System.currentTimeMillis()));
        userNode.put("username", String.valueOf(System.currentTimeMillis()));
        userNode.put("password", String.valueOf(System.currentTimeMillis()));
        userNode.put("photo", "0");
        userNode.setOperate(SdOperateType.create);
        SdData saveData = new SdData();
        saveData.addNodeIfAbsent(userNode);

        File filePhoto = new File(this.getClass().getClassLoader().getResource("zxy_icon.jpg").getFile());

        UXDFBinaryFileInfo fileInfo = new UXDFBinaryFileInfo() {
            @Override
            public String getName() {
                return filePhoto.getName();
            }

            @Override
            public String getContentType() {
                return "image/jpeg";
            }

            @Override
            public long getLength() {
                return filePhoto.length();
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return new FileInputStream(filePhoto);
            }

            @Override
            public boolean isFile() {
                return true;
            }
        };

        UXDFBinaryFileInfo[] files = new UXDFBinaryFileInfo[]{fileInfo};

        SdDataSaveResult saveResult = this.storageService.saveData(saveData, null, files, false);

        System.out.println(JSON.toJSONString(saveResult));
        System.out.println(saveData);
        Assert.assertTrue(saveResult.getNodeCreateNum().get() > 0);
        Assert.assertTrue(IdMaker.effective(saveData.getUnmodifiableNode().get(0).get__Id()));
        return saveData.getUnmodifiableNode().get(0);
    }
}
