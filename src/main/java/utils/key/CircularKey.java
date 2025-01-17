package utils.key;

import utils.docgen.WorkLoadSettings;

public class CircularKey extends RandomKey {
    public CircularKey() {
        super();
    }

    public CircularKey(WorkLoadSettings ws) {
        super(ws);
    }

    public String next(long docIndex) {
        return super.next(docIndex);
    }
}
