package utils.key;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import utils.docgen.WorkLoadSettings;

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

    public void set_total_vbs(int num_vbs) {
        this.total_vbs = num_vbs;
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

    public Map<Long, String> generate_keys_for_target_vbs(Long doc_index, Long num_keys, int[] target_vbs) {
        int keys_generated = 0;
        Integer vb_of_key;
        long key_index = doc_index;
        String key;
        Map<Long, String> generated_keys = new HashMap<Long, String>();

        while(keys_generated < num_keys) {
            key = this.next(key_index);
            key_index ++;

            vb_of_key = this.get_vbucket_for_key(key);
            for (int vb_num : target_vbs) {
                if (vb_num == vb_of_key) {
                    generated_keys.put(doc_index, key);
                    doc_index ++;
                    keys_generated ++;
                    break;
                }
            }
        }
        return generated_keys;
    }
}
