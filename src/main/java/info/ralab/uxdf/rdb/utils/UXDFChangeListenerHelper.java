package info.ralab.uxdf.rdb.utils;

import info.ralab.uxdf.event.UXDFNodeChangeListener;
import org.springframework.context.ApplicationContext;

import java.util.Arrays;

public class UXDFChangeListenerHelper {

    /**
     * 根据Node定义名称获取对应实例变更监听
     *
     * @param applicationContext Spring上下文
     * @param nodeName           Node定义名称
     * @return Node实例变更监听
     */
    public static UXDFNodeChangeListener getNodeChangeListener(
            final ApplicationContext applicationContext,
            final String nodeName
    ) {
        if (applicationContext == null || nodeName == null || nodeName.isEmpty()) {
            return null;
        }
        String[] names = applicationContext.getBeanNamesForType(UXDFNodeChangeListener.class);
        if (names == null || names.length == 0) {
            return null;
        } else {
            Arrays.sort(names);

            if (Arrays.binarySearch(names, nodeName) > -1) {
                return applicationContext.getBean(nodeName, UXDFNodeChangeListener.class);
            } else {
                return null;
            }
        }
    }
}
