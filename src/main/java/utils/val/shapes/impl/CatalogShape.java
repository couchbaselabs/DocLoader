package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.ARRAY_OF_SCALAR;
import static utils.val.shapes.engine.FieldKind.MIXED_TYPE;
import static utils.val.shapes.engine.FieldKind.OBJECT_DYNAMIC_KEYS;
import static utils.val.shapes.engine.FieldKind.SCALAR;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import utils.val.shapes.DocShape;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.DateString;
import utils.val.shapes.annotations.Empty;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.Length;
import utils.val.shapes.annotations.LongRange;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.OneOf;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Precision;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.Shape;

/**
 * Wide, sparse catalog record: many optional attributes of which each document carries only a
 * handful, so documents sharing one _type have very different field sets. Presence
 * probabilities are deliberately tiered to give index tests a spread of selectivities.
 */
@Shape(type = "catalogItem", weight = 6, idPattern = "cat::{sku}::{seq}")
public class CatalogShape implements DocShape {

    static final List<String> SKUS = skus(20000);
    static final List<String> CATEGORIES = Arrays.asList(
            "electronics", "grocery", "apparel", "home", "garden", "toys", "automotive",
            "beauty", "sports", "books", "pet", "office", "hardware", "pharmacy", "outdoor");
    static final List<String> BRANDS = Arrays.asList(
            "Northwind", "Kingsway", "Harlow & Sons", "Belmont", "Ridgeline", "Fairhaven",
            "Copperfield", "Stonebridge", "Ashworth", "Halstead", "Vireo", "Lumen",
            "Arclight", "Meridian", "Cobalt Works", "Ironwood", "Sable", "Verdant",
            "Quarry Lane", "Ember", "Tidewater", "Foxglove", "Pinnacle", "Orchard House",
            "Wren & Co", "Blackthorn", "Silvermoor", "Hollowell", "Marchmont", "Kestrel",
            "Bramble", "Cresswell", "Duneside", "Elmgrove", "Fernhill", "Gatsby Home",
            "Havenbrook", "Inglenook", "Junipero", "Kirkstone", "Larkspur", "Mossvale",
            "Nightingale", "Oakhurst", "Peregrine", "Quillon", "Rosewood", "Stanhope",
            "Thornbury", "Ullswater", "Vanbrugh", "Westerly", "Yarrow", "Zephyr Goods",
            "3M-Style", "A&B Supply", "Böhm Werke", "Ōtani Trading", "Núñez Hermanos", "Çelik Sanayi");
    static final List<String> COLOURS = Arrays.asList(
            "red", "blue", "green", "black", "white", "silver", "gold", "matte black",
            "rose gold", "navy", "olive", "burgundy", "charcoal", "ivory", "sand", "slate",
            "teal", "mustard", "coral", "lilac", "forest green", "midnight blue", "sage",
            "terracotta", "graphite", "champagne", "gunmetal", "pearl white", "espresso",
            "Racing Green", "OFF-WHITE", "jet black", "sky blue", "brushed steel",
            "walnut", "oak", "rose", "amber", "indigo", "crimson");
    static final List<String> WAREHOUSES = ids("WH-", 60);
    static final List<String> SUPPLIERS = ids("SUP-", 400);
    static final List<String> LEGACY_CODES = ids("LGC-", 300);

    // ── near-universal fields (high presence, low selectivity value) ──

    @Path("sku")
    @Kind(SCALAR)
    @ChoiceFrom("SKUS")
    String sku;

    @Path("category")
    @Kind(SCALAR)
    @ChoiceFrom("CATEGORIES")
    String category;

    @Path("active")
    @Kind(SCALAR)
    boolean active;

    // ── common fields (~70-90% present) ──

    @Path("brand")
    @Kind(SCALAR)
    @ChoiceFrom("BRANDS")
    @Optional(prob = 0.1)
    String brand;

    @Path("pricing.list")
    @Kind(SCALAR)
    @Range(min = 0.99, max = 4999.99)
    @Precision(scale = 2)
    @Optional(prob = 0.12)
    BigDecimal listPrice;

    @Path("pricing.cost")
    @Kind(SCALAR)
    @Range(min = 0.25, max = 3500.0)
    @Precision(scale = 4)
    @Optional(prob = 0.3)
    @Nullable(prob = 0.05)
    BigDecimal cost;

    @Path("addedOn")
    @Kind(SCALAR)
    @DateString(formats = {"yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ss'Z'", "dd/MM/yyyy"})
    @Optional(prob = 0.2)
    String addedOn;

    @Path("discontinuedOn")
    @Kind(SCALAR)
    @DateString(formats = {"yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ss'Z'", "dd/MM/yyyy"})
    @Optional(prob = 0.85)
    String discontinuedOn;

    @Path("colours")
    @Kind(ARRAY_OF_SCALAR)
    @ChoiceFrom("COLOURS")
    @ArraySize(min = 1, max = 4)
    @Optional(prob = 0.35)
    @Empty(prob = 0.08)
    List<String> colours;

    // ── uncommon fields (~20-40% present) ──

    @Path("dimensions.weightGrams")
    @Kind(SCALAR)
    @Range(min = 1, max = 250000)
    @Optional(prob = 0.6)
    Integer weightGrams;

    @Path("dimensions.heightMm")
    @Kind(SCALAR)
    @Range(min = 1, max = 4000)
    @Optional(prob = 0.62)
    Integer heightMm;

    @Path("dimensions.widthMm")
    @Kind(SCALAR)
    @Range(min = 1, max = 4000)
    @Optional(prob = 0.62)
    Integer widthMm;

    @Path("supplierId")
    @Kind(SCALAR)
    @ChoiceFrom("SUPPLIERS")
    @Optional(prob = 0.45)
    String supplierId;

    @Path("barcode")
    @Kind(SCALAR)
    @LongRange(min = 1000000000000L, max = 9999999999999L)
    @Optional(prob = 0.4)
    Long barcode;

    // Some records still carry the legacy numeric code, others the newer string form.
    @Path("legacyCode")
    @Kind(MIXED_TYPE)
    @OneOf({String.class, Integer.class, Double.class})
    @ChoiceFrom("LEGACY_CODES")
    @Range(min = 1, max = 99999)
    @Optional(prob = 0.55)
    Object legacyCode;

    // Stock keyed by warehouse id rather than by schema.
    @Path("stockByWarehouse")
    @Kind(OBJECT_DYNAMIC_KEYS)
    @ChoiceFrom("WAREHOUSES")
    @ArraySize(min = 1, max = 5)
    @Optional(prob = 0.4)
    Map<String, Stock> stockByWarehouse;

    // ── rare fields (~2-10% present: high-selectivity index targets) ──

    @Path("recallNotice")
    @Kind(SCALAR)
    @Length(min = 40, max = 200, largeProb = 0.1, largeMax = 8192)
    @Optional(prob = 0.97)
    String recallNotice;

    @Path("hazmatClass")
    @Kind(SCALAR)
    @ChoiceFrom("CATEGORIES")
    @Optional(prob = 0.95)
    String hazmatClass;

    @Path("exportRestricted")
    @Kind(SCALAR)
    @Optional(prob = 0.93)
    Boolean exportRestricted;

    @Path("clearanceReason")
    @Kind(SCALAR)
    @Length(min = 10, max = 80)
    @Optional(prob = 0.9)
    @Nullable(prob = 0.1)
    String clearanceReason;

    @Path("notes")
    @Kind(SCALAR)
    @Length(min = 20, max = 120, largeProb = 0.05, largeMax = 16384)
    @Optional(prob = 0.8)
    String notes;

    public static class Stock {
        @Path("onHand")
        @Kind(SCALAR)
        @Range(min = 0, max = 5000)
        int onHand;

        @Path("reserved")
        @Kind(SCALAR)
        @Range(min = 0, max = 500)
        @Optional(prob = 0.3)
        Integer reserved;

        @Path("lastCounted")
        @Kind(SCALAR)
        @DateString(format = "yyyy-MM-dd")
        @Optional(prob = 0.5)
        String lastCounted;
    }

    /** Mostly uniform, with a minority of legacy, lowercase and differently-padded forms. */
    private static List<String> ids(String prefix, int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 19 == 0) {
                pool.add(prefix.toLowerCase() + i);
            } else if (i % 11 == 0) {
                pool.add("OLD-" + prefix + String.format("%04d", i));
            } else if (i % 7 == 0) {
                pool.add(prefix + String.format("%06d", i));
            } else {
                pool.add(prefix + String.format("%04d", i));
            }
        }
        return pool;
    }

    /** Mixed SKU conventions, as accumulated by a catalogue that has survived migrations. */
    private static List<String> skus(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 23 == 0) {
                pool.add("sku_" + i);
            } else if (i % 11 == 0) {
                pool.add(String.valueOf(100000 + i));
            } else if (i % 5 == 0) {
                pool.add("SKU-" + String.format("%08d", i));
            } else {
                pool.add("SKU-" + String.format("%06d", i));
            }
        }
        return pool;
    }
}
