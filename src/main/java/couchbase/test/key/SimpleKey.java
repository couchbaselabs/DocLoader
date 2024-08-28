package couchbase.test.key;

import java.util.zip.CRC32;
import couchbase.test.docgen.WorkLoadSettings;

public class SimpleKey {
    public WorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    String alphabet = "";
    public int key_counter = 0;
    private static int total_vbs = 1024;

    public SimpleKey() {
        super();
    }

    public static boolean contains(int[] array, int target) {
        for (int num : array)
            if (num == target)
                return true;
        return false;
    }

    public SimpleKey(WorkLoadSettings ws) {
        super();
        this.ws = ws;
    }

    public int get_vbucket_for_key(String key) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes());
        return ((((int)crc.getValue() >> 16) & 0x7fff) & (total_vbs-1));
    }

    public String next(long doc_index) {
        int counterSize = Long.toString(Math.abs(doc_index)).length();
        int padd = this.ws.keySize - this.ws.keyPrefix.length() - counterSize;
        return this.ws.keyPrefix + this.padding.substring(0, padd) + Math.abs(doc_index);
    }
}
