package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.ARRAY_OF_ARRAY_OF_OBJECT;
import static utils.val.shapes.engine.FieldKind.SCALAR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import utils.val.shapes.DocShape;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.DateString;
import utils.val.shapes.annotations.Empty;
import utils.val.shapes.annotations.Identity;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.LongRange;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.Shape;

/**
 * Mobile document carrying sync metadata in the document body, mimicking the shape of
 * _sync xattrs. Revisions are an array of arrays of objects (array -> array -> object).
 */
@Shape(type = "mobile", weight = 8, idPattern = "mob::{channel}::{seq}")
public class MobileShape implements DocShape {

    static final List<String> CHANNELS = Arrays.asList(
            "public", "private", "beta", "eu-west-1", "eu-central-1", "us-east-1", "us-west-2",
            "ap-south-1", "ap-southeast-2", "internal", "staff", "partners", "trial",
            "enterprise", "legacy");
    static final List<String> ATTACHMENT_NAMES = Arrays.asList(
            "img1.jpg", "img2.png", "receipt.pdf", "voice.m4a", "thumb.webp", "export.csv",
            "scan 001.tiff", "contract-final.docx", "résumé.pdf", "写真.jpg", "backup.tar.gz",
            "avatar.gif", "statement Q3.xlsx", "signature.svg", "video-clip.mp4",
            "notes.txt", "map.geojson", "trace.log", "profile.vcf", "invoice_2026.pdf");
    static final List<String> REVISIONS = revisions(2000);

    @Path("userEmail")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.EMAIL)
    String userEmail;

    @Path("channel")
    @Kind(SCALAR)
    @ChoiceFrom("CHANNELS")
    String channel;

    @Path("syncInfo.rev")
    @Kind(SCALAR)
    @ChoiceFrom("REVISIONS")
    String rev;

    @Path("syncInfo.sequence")
    @Kind(SCALAR)
    @LongRange(min = 9007199254740993L, max = 9223372036854775000L)
    long sequence;

    @Path("syncInfo.timeSaved")
    @Kind(SCALAR)
    @DateString(formats = {
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ssxxx"},
            epochProb = 0.1)
    String timeSaved;

    @Path("syncInfo.deletedAt")
    @Kind(SCALAR)
    @DateString(format = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    @Optional(prob = 0.8)
    String deletedAt;

    @Path("syncInfo.channels")
    @Kind(SCALAR)
    @ChoiceFrom("CHANNELS")
    @Nullable(prob = 0.05)
    String syncChannel;

    @Path("syncInfo.revisions")
    @Kind(ARRAY_OF_ARRAY_OF_OBJECT)
    @ArraySize(outerMin = 1, outerMax = 3, min = 1, max = 2)
    @Empty(prob = 0.06)
    List<List<Attachment>> revisionHistory;

    public static class Attachment {
        @Path("name")
        @Kind(SCALAR)
        @ChoiceFrom("ATTACHMENT_NAMES")
        String name;

        @Path("size")
        @Kind(SCALAR)
        @Range(min = 1024, max = 5242880)
        int size;

        @Path("stub")
        @Kind(SCALAR)
        boolean stub;
    }

    private static List<String> revisions(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            // Revision generations start at 1; a "0-" revision never exists in Sync Gateway.
            String hash = Integer.toHexString(i * 7919);
            pool.add((i % 400 + 1) + "-" + hash.substring(0, Math.min(8, hash.length())));
        }
        return pool;
    }
}
