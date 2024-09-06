package couchbase.test.sdk;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import com.couchbase.client.core.error.DecodingFailureException;
import com.couchbase.client.core.error.subdoc.PathNotFoundException;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;


public class SubDocOps {
    public List<HashMap<String, Object>> bulkSubDocOperation(Collection collection,
                                                             List<Tuple2<String, List<MutateInSpec>>> mutateInSpecs,
                                                             MutateInOptions t_mutateInOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<HashMap<String, Object>> returnValue = Flux.fromIterable(mutateInSpecs)
                .flatMap(new Function<Tuple2<String, List<MutateInSpec>>, Publisher<HashMap<String, Object>>>() {
                    public Publisher<HashMap<String, Object>> apply(Tuple2<String, List<MutateInSpec>> subDocOperations) {
                        final String id = subDocOperations.getT1();
                        final List<MutateInSpec> subDocOps = subDocOperations.getT2();
                        final HashMap<String, Object> returnValue = new HashMap<String, Object>();
                        returnValue.put("error", null);
                        returnValue.put("cas", 0);
                        returnValue.put("status", true);
                        returnValue.put("id", id);
                        returnValue.put("result", null);
                        return reactiveCollection.mutateIn(id, subDocOps, t_mutateInOptions)
                                .map(new Function<MutateInResult, HashMap<String, Object>>() {
                                    public HashMap<String, Object> apply(MutateInResult result) {
                                        returnValue.put("result", result);
                                        returnValue.put("cas", result.cas());
                                        return returnValue;
                                    }
                                }).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>(){
                                    public Mono<HashMap<String, Object>> apply(Throwable error) {
                                        returnValue.put("error", error);
                                        returnValue.put("status", false);
                                        return Mono.just(returnValue);
                                    }
                                });
                    }
                }).subscribeOn(Schedulers.parallel()).collectList().block();
        return returnValue;
    }

    public List<HashMap<String, Object>> bulkGetSubDocOperation(Collection collection,
                                                                List<Tuple2<String, List<LookupInSpec>>> keys,
                                                                LookupInOptions lookupInOptions) {
        final ReactiveCollection reactiveCollection = collection.reactive();
        List<HashMap<String, Object>> returnValue = Flux.fromIterable(keys)
            .flatMap(new Function<Tuple2<String, List<LookupInSpec>>, Publisher<HashMap<String, Object>>>() {
                public Publisher<HashMap<String, Object>> apply(Tuple2<String, List<LookupInSpec>> subDocOperations) {
                    final String id = subDocOperations.getT1();
                    final List<LookupInSpec> lookUpInSpecs = subDocOperations.getT2();
                    final HashMap<String, Object> retVal = new HashMap<String, Object>();
                    final HashMap<String, Object> sd_result = new HashMap<String, Object>();
                    retVal.put("id", id);
                    retVal.put("cas", 0);
                    retVal.put("value", sd_result);
                    retVal.put("error", null);
                    retVal.put("status", true);
                    return reactiveCollection.lookupIn(id, lookUpInSpecs, lookupInOptions)
                        .map(new Function<LookupInResult, HashMap<String, Object>>() {
                            public HashMap<String, Object> apply(LookupInResult optionalResult) {
                                retVal.put("cas", optionalResult.cas());
                                List<Object> content = new ArrayList<Object>();
                                for (int i=0; i<lookUpInSpecs.size(); i++) {
                                    try {
                                        // sd_result.put();
                                        content.add(optionalResult.contentAsObject(i));
                                    } catch (DecodingFailureException e1) {
                                        try {
                                            content.add(optionalResult.contentAsArray(i));
                                        } catch (DecodingFailureException e2) {
                                            try {
                                                content.add(optionalResult.contentAs(i, Integer.class));
                                            }
                                            catch (DecodingFailureException e3) {
                                                try {
                                                    content.add(optionalResult.contentAs(i, String.class));
                                                } catch (Exception e4) {
                                                    content.add(null);
                                                }
                                            }
                                        }
                                    } catch (PathNotFoundException e1) {
                                        retVal.put("status", false);
                                        // Check is to track only the first known error
                                        if(retVal.get("error") == null)
                                            retVal.put("error", e1);
                                    }
                                }
                                retVal.put("content", content);
                                return retVal;
                            }
                        }).onErrorResume(new Function<Throwable, Mono<HashMap<String, Object>>>() {
                            public Mono<HashMap<String, Object>> apply(Throwable error) {
                                retVal.put("error", error);
                                return Mono.just(retVal);
                            }
                        }).defaultIfEmpty(retVal);
                }
            }).subscribeOn(Schedulers.parallel()).collectList().block();
        return returnValue;
    }
}
