package st.mou.opensymphony.module.propertyset.memory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.opensymphony.module.propertyset.DuplicatePropertyKeyException;
import com.opensymphony.module.propertyset.IllegalPropertyException;

public class SerializablePropertySet extends MemoryPropertySet implements Serializable {
    private HashMap serialMap;

    public void init(Map config, Map args) {
        serialMap = new HashMap();
    }

    protected void setImpl(int type, String key, Object value) throws IllegalPropertyException, DuplicatePropertyKeyException {
        if ((value != null) && !(value instanceof Serializable)) {
            throw new IllegalPropertyException("Cannot set " + key + ". Value type " + value.getClass() + " not Serializable");
        }

        super.setImpl(type, key, value);
    }

    protected HashMap getMap() {
        return serialMap;
    }
}
