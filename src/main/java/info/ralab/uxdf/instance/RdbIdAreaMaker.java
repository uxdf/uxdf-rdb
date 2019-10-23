package info.ralab.uxdf.instance;

import info.ralab.uxdf.rdb.mapper.UXDFMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("RdbIdAreaMaker")
public class RdbIdAreaMaker implements IdAreaMaker {

    private UXDFMapper uxdfMapper;

    @Autowired
    public RdbIdAreaMaker(UXDFMapper uxdfMapper) {
        this.uxdfMapper = uxdfMapper;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public String next() {
        String area = this.uxdfMapper.getIdArea();
        if (area == null) {
            area = IdMaker.MIN_VALUE_STRING;
            this.uxdfMapper.addIdArea(area);
        }
        area = Long.toString(Long.parseLong(area, IdMaker.RADIX) + 1, IdMaker.RADIX);
        area = IdMaker.fillDigits(area);
        this.uxdfMapper.setIdArea(area);
        return area;
    }
}
