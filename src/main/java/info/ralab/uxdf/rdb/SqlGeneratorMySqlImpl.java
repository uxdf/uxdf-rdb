package info.ralab.uxdf.rdb;

import com.google.common.base.Joiner;
import info.ralab.uxdf.rdb.model.DatabaseType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("mysqlSqlGenerator")
@Slf4j
public class SqlGeneratorMySqlImpl extends SqlGeneratorCommon {

    @Autowired
    public SqlGeneratorMySqlImpl(NameStrategy nameStrategy, TemplateService templateService) {
        super(nameStrategy, templateService);
    }

    @Override
    protected String getTemplatePath() {
        return Joiner.on("").join(
                TEMPLATE_DIR,
                TEMPLATE_PREFIX,
                DatabaseType.MYSQL.name().toLowerCase(),
                TEMPLATE_SUFFIX
        );
    }
}
