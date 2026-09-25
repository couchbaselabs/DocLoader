package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.OBJECT_DYNAMIC_KEYS;
import static utils.val.shapes.engine.FieldKind.RECURSIVE_SELF;
import static utils.val.shapes.engine.FieldKind.SCALAR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import utils.val.shapes.DocShape;
import utils.val.shapes.annotations.ArraySize;
import utils.val.shapes.annotations.ChoiceFrom;
import utils.val.shapes.annotations.Empty;
import utils.val.shapes.annotations.Identity;
import utils.val.shapes.annotations.Kind;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Range;
import utils.val.shapes.annotations.RecursionDepth;
import utils.val.shapes.annotations.Shape;

/**
 * Telecom assigned product: self-similar arrays nested to a randomised depth, plus an object
 * keyed by service identifier rather than by schema, for OBJECT_PAIRS and PAIRS(SELF) tests.
 */
@Shape(type = "assignedProduct", weight = 20, idPattern = "asgn::{serviceId}::{seq}")
public class AssignedProductShape implements DocShape {

    static final List<String> SERVICE_IDS = serviceIds(10000);
    static final List<String> PRODUCT_NAMES = Arrays.asList(
            "Broadband 36", "Broadband 100", "Broadband 250", "Broadband 500", "Fibre 1G",
            "Fibre 2G", "Full Fibre 900", "ADSL Legacy", "Mobile 5GB", "Mobile 20GB",
            "Mobile 100GB", "Mobile Unlimited", "Mobile Data SIM", "eSIM Secondary",
            "TV Basic", "TV Entertainment", "TV Sports", "TV Sports HD", "TV Movies",
            "TV Premium", "TV Kids", "4K Upgrade", "Multiroom Box", "Catch-up Plus",
            "Landline", "Landline Anytime", "International Calls", "Voicemail Plus",
            "Call Divert", "Caller ID", "Static IP", "Static IP Block /29", "IPv6 Allocation",
            "Cloud Backup 100GB", "Cloud Backup 1TB", "Security Suite", "Parental Controls",
            "Roaming Add-on EU", "Roaming Add-on Global", "Router Rental", "Mesh Extender",
            "Engineer Visit", "Priority Support", "Business SLA 4hr", "Business SLA 8hr",
            "Hosted PBX Seat", "SIP Trunk 10ch", "Leased Line 100M", "Wi-Fi Guest Portal",
            "Device Insurance");
    static final List<String> STATUSES = Arrays.asList(
            "active", "suspended", "pending", "cancelled", "provisioning", "ceased");

    @Path("serviceIdentifier.id")
    @Kind(SCALAR)
    @ChoiceFrom("SERVICE_IDS")
    String serviceId;

    @Path("subscriberEmail")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.EMAIL)
    String subscriberEmail;

    @Path("status")
    @Kind(SCALAR)
    @ChoiceFrom("STATUSES")
    @Nullable(prob = 0.05)
    String status;

    @Path("containAssignedProduct")
    @Kind(RECURSIVE_SELF)
    @RecursionDepth(min = 1, max = 4, continueProb = 0.6, childrenMin = 1, childrenMax = 2)
    @Empty(prob = 0.1)
    List<AssignedProductShape> containAssignedProduct;

    // Keys are data, not schema: { "G00000074": { ... }, "G00000512": { ... } }
    @Path("productsByService")
    @Kind(OBJECT_DYNAMIC_KEYS)
    @ChoiceFrom("SERVICE_IDS")
    @ArraySize(min = 1, max = 4)
    Map<String, ProductDetail> productsByService;

    public static class ProductDetail {
        @Path("name")
        @Kind(SCALAR)
        @ChoiceFrom("PRODUCT_NAMES")
        String name;

        @Path("monthlyPrice")
        @Kind(SCALAR)
        @Range(min = 5.0, max = 250.0)
        Double monthlyPrice;

        @Path("active")
        @Kind(SCALAR)
        boolean active;
    }

    private static List<String> serviceIds(int count) {
        List<String> pool = new ArrayList<String>(count);
        for (int i = 1; i <= count; i++) {
            pool.add("G" + String.format("%08d", i));
        }
        return pool;
    }
}
