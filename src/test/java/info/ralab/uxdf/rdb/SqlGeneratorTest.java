package info.ralab.uxdf.rdb;

import info.ralab.uxdf.UXDFLoader;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
@Slf4j
public class SqlGeneratorTest {

    @Autowired
    @Qualifier("mysqlSqlGenerator")
    private SqlGenerator mysqlSqlGenerator;
    @Autowired
    @Qualifier("oracleSqlGenerator")
    private SqlGenerator oracleSqlGenerator;

    @Test
    public void testGenerateDDL() {
        String ddlMysql = this.mysqlSqlGenerator.generateDDL(UXDFLoader.getSd());
        System.out.println(ddlMysql);

        String ddlOracle = this.oracleSqlGenerator.generateDDL(UXDFLoader.getSd());
        System.out.println(ddlOracle);
    }
}
