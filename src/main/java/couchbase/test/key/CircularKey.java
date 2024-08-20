package couchbase.test.key;

import couchbase.test.docgen.WorkLoadSettings;

public class CircularKey extends RandomKey {
    // default :: -1 means infinite load
    private int iterations = -1;

    public CircularKey() {
        super();
    }

    public CircularKey(WorkLoadSettings ws) {
        super(ws);
    }

    public CircularKey(int iterations) {
        super();
        this.iterations = iterations;
    }

    public CircularKey(WorkLoadSettings ws, int iterations) {
        super(ws);
        this.iterations = iterations;
    }

    public String next(long docIndex) {
        return super.next(docIndex);
    }

    public boolean checkIterations() {
        // Can be both -1 (Infinite load) or greater than 1 (iterations set by user)
        if (this.iterations != 0) {
            if (this.iterations >= 1)
                // Num_iterations set by user, so decrement by one
                this.iterations -= 1;
            return true;
        }
        return false;
    }
}
