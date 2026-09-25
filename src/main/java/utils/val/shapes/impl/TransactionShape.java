package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.ARRAY_OF_OBJECT;
import static utils.val.shapes.engine.FieldKind.MIXED_TYPE;
import static utils.val.shapes.engine.FieldKind.OBJECT;
import static utils.val.shapes.engine.FieldKind.SCALAR;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import utils.val.shapes.DocShape;
import utils.val.shapes.annotations.After;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.DateString;
import utils.val.shapes.annotations.Identity;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.OneOf;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Precision;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.Sequence;
import utils.val.shapes.annotations.Shape;

/**
 * Retail transaction: deep nesting with ._value leaf-wrapping, array-of-objects line items,
 * an optional tender branch, a coherent start/end date pair, and an amount whose type varies
 * across documents.
 */
@Shape(type = "transaction", weight = 40, idPattern = "txn::{storeId}::{seq}")
public class TransactionShape implements DocShape {

    private static final String STAMP = "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'";

    static final List<String> STORE_IDS = storeIds(500);
    static final List<String> BU_IDS = ids("BU-", 80);
    static final List<String> ITEM_IDS = ids("I-", 5000);
    static final List<String> AMOUNTS_AS_TEXT = amountsAsText(200);

    @Path("storeId")
    @Kind(SCALAR)
    @ChoiceFrom("STORE_IDS")
    String storeId;

    // The same person appears on their profile, their media records and here.
    @Path("Customer.EmailAddress._value")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.EMAIL)
    String customerEmail;

    @Path("Customer.Name._value")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.FULL_NAME)
    @Optional(prob = 0.2)
    String customerName;

    @Path("BusinessUnit.UnitID._value")
    @Kind(SCALAR)
    @ChoiceFrom("BU_IDS")
    String unitId;

    @Path("Transaction.StartDateTime._value")
    @Kind(SCALAR)
    @DateString(format = STAMP)
    String startDateTime;

    // Always later than the start, so range predicates over the pair return sensible results.
    // Minutes rather than days: a point-of-sale transaction lasts minutes, not days.
    @Path("Transaction.EndDateTime._value")
    @Kind(SCALAR)
    @DateString(format = STAMP)
    @After(value = "startDateTime", minMinutes = 1, maxMinutes = 45)
    String endDateTime;

    @Path("Transaction.Total._value")
    @Kind(SCALAR)
    @Range(min = 1.0, max = 25000.0)
    @Precision(scale = 4)
    BigDecimal total;

    // Mostly 1-4 items, but ~1% of transactions carry a very large basket.
    @Path("Transaction.RetailTransaction.LineItem")
    @Kind(ARRAY_OF_OBJECT)
    @ArraySize(min = 1, max = 4, largeProb = 0.01, largeMax = 200)
    List<LineItem> lineItems;

    @Path("Transaction.TenderControlTransaction")
    @Kind(OBJECT)
    @Optional(prob = 0.5)
    Tender tender;

    public static class LineItem {
        @Path("seq")
        @Kind(SCALAR)
        @Sequence
        int seq;

        @Path("itemId")
        @Kind(SCALAR)
        @ChoiceFrom("ITEM_IDS")
        String itemId;

        // Number in most documents, string in the rest: the classic amount-as-text motif.
        @Path("SaleReturn.ExtendedAmount._value")
        @Kind(MIXED_TYPE)
        @OneOf({Double.class, String.class})
        @Range(min = 1.0, max = 500.0)
        @ChoiceFrom("AMOUNTS_AS_TEXT")
        @Optional(prob = 0.3)
        Object extendedAmount;
    }

    public static class Tender {
        @Path("DrawerSettle.Amount._value")
        @Kind(SCALAR)
        @Range(min = 1, max = 1000)
        int amount;

        @Path("SafeSettle.Amount._value")
        @Kind(SCALAR)
        @Range(min = 1, max = 1000)
        @Nullable(prob = 0.3)
        Integer safeSettle;
    }

    /** Mostly uniform, with a minority of legacy and lowercase forms. */
    private static List<String> ids(String prefix, int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 17 == 0) {
                pool.add(prefix.toLowerCase() + i);
            } else if (i % 9 == 0) {
                pool.add(prefix + String.format("%05d", i));
            } else {
                pool.add(prefix + String.format("%03d", i));
            }
        }
        return pool;
    }

    /** Mostly uniform, with a deliberate minority of legacy and lowercase formats. */
    private static List<String> storeIds(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            if (i % 20 == 0) {
                pool.add("OLD-S-" + i);
            } else if (i % 13 == 0) {
                pool.add("s-" + String.format("%03d", i));
            } else if (i % 7 == 0) {
                pool.add("S-" + String.format("%05d", i));
            } else {
                pool.add("S-" + String.format("%03d", i));
            }
        }
        return pool;
    }

    /**
     * The string arm of the mixed-type amount. Real feeds store money as text in whatever
     * shape the upstream system used, so the formats vary rather than the values.
     */
    private static List<String> amountsAsText(int count) {
        String[] shapes = {
                "%1$d.%2$02d", "%1$d,%2$02d", "$%1$d.%2$02d", "%1$d.%2$02d USD",
                " %1$d.%2$02d ", "%1$d.%2$01d", "00%1$d.%2$02d", "%1$d", "-%1$d.%2$02d",
                "%1$d.%2$02d0", "\u00a3%1$d.%2$02d", "%1$d.%2$02d EUR"};
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            int whole = (i * 37) % 5000 + 1;
            int cents = (i * 13) % 100;
            String v = String.format(shapes[i % shapes.length], whole, cents);
            if (whole >= 1000 && i % 5 == 0) {
                v = v.replaceFirst("(\\d)(\\d{3})", "$1,$2");
            }
            pool.add(v);
        }
        return pool;
    }
}
