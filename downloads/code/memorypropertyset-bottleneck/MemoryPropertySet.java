package st.mou.opensymphony.module.propertyset.memory;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.opensymphony.module.propertyset.AbstractPropertySet;
import com.opensymphony.module.propertyset.DuplicatePropertyKeyException;
import com.opensymphony.module.propertyset.InvalidPropertyTypeException;

public class MemoryPropertySet extends AbstractPropertySet {

    private HashMap map;
    private ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private Lock readLock = readWriteLock.readLock();
    private Lock writeLock = readWriteLock.writeLock();

    public Collection getKeys(String prefix, int type) {
        List result = new LinkedList();
        readLock.lock();
        try {
            Iterator keys = getMap().keySet().iterator();

            while (keys.hasNext()) {
                String key = (String) keys.next();

                if ((prefix == null) || key.startsWith(prefix)) {
                    if (type == 0) {
                        result.add(key);
                    } else {
                        ValueEntry v = (ValueEntry) getMap().get(key);

                        if (v.type == type) {
                            result.add(key);
                        }
                    }
                }
            }

            Collections.sort(result);

            return result;
        } finally {
            readLock.unlock();
        }
    }

    public int getType(String key) {
        readLock.lock();
        try {
            if (getMap().containsKey(key)) {
                return ((ValueEntry) getMap().get(key)).type;
            } else {
                return 0;
            }
        } finally {
            readLock.unlock();
        }
    }

    public boolean exists(String key) {
        return getType(key) > 0;
    }

    public void init(Map config, Map args) {
        map = new HashMap();
    }

    public void remove(String key) {
        writeLock.lock();
        try {
            getMap().remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    protected void setImpl(int type, String key, Object value) throws DuplicatePropertyKeyException {
        writeLock.lock();
        try {
            if (exists(key)) {
                ValueEntry v = (ValueEntry) getMap().get(key);

                if (v.type != type) {
                    throw new DuplicatePropertyKeyException();
                }

                v.value = value;
            } else {
                getMap().put(key, new ValueEntry(type, value));
            }
        } finally {
            writeLock.unlock();
        }

        return;
    }

    protected HashMap getMap() {
        return map;
    }

    protected Object get(int type, String key) throws InvalidPropertyTypeException {
        readLock.lock();
        try {
            if (exists(key)) {
                ValueEntry v = (ValueEntry) getMap().get(key);

                if (v.type != type) {
                    throw new InvalidPropertyTypeException();
                }

                return v.value;
            } else {
                return null;
            }
        } finally {
            readLock.unlock();
        }
    }

    public static final class ValueEntry implements Serializable {
        Object value;
        int type;

        public ValueEntry() {
        }

        public ValueEntry(int type, Object value) {
            this.type = type;
            this.value = value;
        }

        public void setType(int type) {
            this.type = type;
        }

        public void setValue(Object value) {
            this.value = value;
        }
    }
}
