package utils.docgen;

import java.util.List;

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonGetter;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

public class anySize {

    public static class Person1 {
        private String body;
        private int mutated;

        @JsonCreator
        public
        Person1(
                @JsonProperty("mutated") int mutated,
                @JsonProperty("body") String body) {
            this.mutated = mutated;
            this.body = body;
        }

        @JsonGetter
        public String body() {
            return body;
        }

        @JsonGetter
        public int mutated() {
            return mutated;
        }
    }
}
