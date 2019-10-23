package info.ralab.uxdf.rdb;

import com.google.common.base.Joiner;
import info.ralab.uxdf.rdb.model.DatabaseType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("oracleSqlGenerator")
@Slf4j
public class SqlGeneratorOracleImpl extends SqlGeneratorCommon {

    @Autowired
    public SqlGeneratorOracleImpl(NameStrategy nameStrategy, TemplateService templateService) {
        super(nameStrategy, templateService);
    }

    @Override
    protected String getTemplatePath() {
        return Joiner.on("").join(
                TEMPLATE_DIR,
                TEMPLATE_PREFIX,
                DatabaseType.ORACLE.name().toLowerCase(),
                TEMPLATE_SUFFIX
        );
    }
}
