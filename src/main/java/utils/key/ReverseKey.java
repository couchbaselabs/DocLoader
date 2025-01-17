package utils.key;

import utils.docgen.WorkLoadSettings;

public class ReverseKey {
    public WorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    String alphabet = "";

    public ReverseKey() {
        super();
    }

    public ReverseKey(WorkLoadSettings ws) {
        super();
        this.ws = ws;
    }

    public String next(int doc_index) {
        int counterSize = Integer.toString(doc_index).length();
        int padd = this.ws.keySize - this.ws.keyPrefix.length() - counterSize; 
        return this.ws.keyPrefix + this.padding.substring(0, padd) + doc_index;
    }
}