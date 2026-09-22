package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.ARRAY_OF_SCALAR;
import static utils.val.shapes.engine.FieldKind.MIXED_TYPE;
import static utils.val.shapes.engine.FieldKind.OBJECT;
import static utils.val.shapes.engine.FieldKind.SCALAR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import utils.val.shapes.DocShape;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.Empty;
import utils.val.shapes.annotations.Identity;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.OneOf;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.Shape;

/**
 * Content document: the partial-index target. Boolean flags plus a nested object holding a
 * scalar array (object -> object -> array), and an id whose type varies across documents.
 */
@Shape(type = "media_element", weight = 12, idPattern = "media::{caseId}::{seq}")
public class ContentShape implements DocShape {

    static final List<String> LIBRARY_LABELS = Arrays.asList(
            "L1", "L2", "L3", "L4", "archive", "staging", "production", "legal-hold",
            "quarantine", "review-queue", "published", "draft-pool", "syndication",
            "partner-feed", "internal-only", "expired", "restricted", "public-domain",
            "licensed", "user-generated");
    static final List<String> CASE_IDS = caseIds(1000);
    static final List<String> LABELS = Arrays.asList(
            "news", "video", "audio", "image", "transcript", "draft", "published",
            "interview", "archive-footage", "b-roll", "graphic", "podcast", "livestream",
            "highlight", "promo", "trailer", "documentary", "sports", "weather", "breaking",
            "opinion", "analysis", "obituary", "correction", "sponsored");
    static final List<String> EXTERNAL_IDS = externalIds(500);

    @Path("userEmail")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.EMAIL)
    String userEmail;

    @Path("libraryLabel")
    @Kind(SCALAR)
    @ChoiceFrom("LIBRARY_LABELS")
    String libraryLabel;

    @Path("caseId")
    @Kind(SCALAR)
    @ChoiceFrom("CASE_IDS")
    String caseId;

    // Legacy numeric ids alongside newer string ids on the same path.
    @Path("externalId")
    @Kind(MIXED_TYPE)
    @OneOf({String.class, Integer.class})
    @ChoiceFrom("EXTERNAL_IDS")
    @Range(min = 1, max = 999999)
    @Optional(prob = 0.35)
    Object externalId;

    @Path("deleted")
    @Kind(SCALAR)
    boolean deleted;

    @Path("global")
    @Kind(SCALAR)
    @Optional(prob = 0.25)
    Boolean global;

    @Path("classification")
    @Kind(OBJECT)
    Classification classification;

    public static class Classification {
        @Path("labels")
        @Kind(ARRAY_OF_SCALAR)
        @ChoiceFrom("LABELS")
        @ArraySize(min = 1, max = 3)
        @Empty(prob = 0.07)
        List<String> labels;

        @Path("confidence")
        @Kind(SCALAR)
        @Range(min = 0.0, max = 1.0)
        @Nullable(prob = 0.1)
        Double confidence;
    }

    private static List<String> caseIds(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 17 == 0) {
                pool.add("case_" + i);
            } else {
                pool.add("C" + String.format("%04d", i));
            }
        }
        return pool;
    }

    /** Accumulated across three generations of upstream system. */
    private static List<String> externalIds(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 13 == 0) {
                pool.add("ext_" + i);
            } else if (i % 7 == 0) {
                pool.add("EXT" + String.format("%08d", i));
            } else {
                pool.add("EXT-" + String.format("%06d", i));
            }
        }
        return pool;
    }
}
