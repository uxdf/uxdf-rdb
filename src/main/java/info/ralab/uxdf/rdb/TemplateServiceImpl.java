package info.ralab.uxdf.rdb;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.ui.freemarker.FreeMarkerTemplateUtils;

import java.io.IOException;
import java.util.Map;

/**
 * <p>模板解析实现</p>
 * <pre>
 * 默认的模板解析实现
 * 使用 Freemark 作为模板实现
 * 所有的模板放置于 resources/templates 目录下
 * </pre>
 * Copyright: Copyright (c) 2017
 * Company:
 * TemplateServiceImpl.java
 *
 * @author pengqin
 * @version 1.0
 */
@Service
public class TemplateServiceImpl implements TemplateService {

    @Autowired
    Configuration configuration;

    @Override
    public String resolve(String templatePath, Map<String, Object> context) throws info.ralab.uxdf.rdb.exception.TemplateException {
        try {
            Template template = configuration.getTemplate(templatePath);
            return FreeMarkerTemplateUtils.processTemplateIntoString(template, context);
        } catch (IOException | TemplateException e) {
            throw new info.ralab.uxdf.rdb.exception.TemplateException(e.getMessage());
        }

    }

}
