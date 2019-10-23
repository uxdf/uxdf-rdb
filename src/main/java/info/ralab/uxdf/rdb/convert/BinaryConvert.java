package info.ralab.uxdf.rdb.convert;

import info.ralab.uxdf.instance.SdEntity;
import info.ralab.uxdf.utils.UXDFBinaryFileInfo;
import info.ralab.uxdf.utils.UXDFValueConvert;

/**
 * 二进制数据转换
 */
public class BinaryConvert implements UXDFValueConvert<UXDFBinaryFileInfo> {

    public static final String SD_PROP_FILES = "$files";

    private SdEntity sdEntity;

    public BinaryConvert(final SdEntity sdEntity) {
        this.sdEntity = sdEntity;
    }

    @Override
    public UXDFBinaryFileInfo convert(Object value) {
        // 如果当前实体没有文件，则返回NULL
        if (this.sdEntity == null ||
                !this.sdEntity.containsKey(SD_PROP_FILES) ||
                !(this.sdEntity.get(SD_PROP_FILES) instanceof UXDFBinaryFileInfo[])) {
            return null;
        }

        // 获取属性对应文件的下标
        int fileIndex;
        if (value instanceof String && ((String) value).matches("\\d+")) {
            fileIndex = Integer.valueOf((String) value);
        } else if (value instanceof Integer) {
            fileIndex = (Integer) value;
        } else if (value instanceof Long) {
            fileIndex = ((Long) value).intValue();
        } else {
            return null;
        }
        UXDFBinaryFileInfo[] files = (UXDFBinaryFileInfo[]) this.sdEntity.get(SD_PROP_FILES);
        if (files.length > fileIndex && fileIndex > -1) {
            return files[fileIndex];
        }
        return null;
    }
}
