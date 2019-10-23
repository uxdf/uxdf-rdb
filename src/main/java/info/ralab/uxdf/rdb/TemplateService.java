package info.ralab.uxdf.rdb;

import info.ralab.uxdf.rdb.exception.TemplateException;

import java.util.Map;

/**
 * <p>模板解析服务</p>
 * Copyright: Copyright (c) 2017
 * Company:   
 * TemplateService.java
 * @author pengqin
 * @version 1.0
 */
public interface TemplateService {

    /**
     * <p>解析模板</p>
     * templateName 模板路径
     * context 模板解析数据
     * @param templatePath
     * @param context
     * @return
     */
    String resolve(String templatePath, Map<String, Object> context) throws TemplateException;
}
