package utils.val;

import utils.docgen.WorkLoadSettings;

import java.util.Collections;
import java.util.Map;

public class EmptyValue {
    public WorkLoadSettings ws;

    public EmptyValue(WorkLoadSettings ws) {
        this.ws = ws;
    }

    public Map<String, Object> next(String key) {
        return Collections.emptyMap();
    }
}
