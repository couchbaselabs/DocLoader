package utils.val.shapes.impl;

import static utils.val.shapes.engine.FieldKind.ARRAY_OF_OBJECT;
import static utils.val.shapes.engine.FieldKind.ARRAY_OF_SCALAR;
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
import utils.val.shapes.annotations.Length;
import utils.val.shapes.annotations.LongRange;
import utils.val.shapes.annotations.Nullable;
import utils.val.shapes.annotations.Optional;
import utils.val.shapes.annotations.Path;
import utils.val.shapes.annotations.Shape;

/**
 * Profile / conversation: mixed-case email for LOWER() matching, scalar arrays, and an array
 * of objects whose elements carry their own scalar array (object -> array -> array).
 */
@Shape(type = "profile", weight = 20, idPattern = "prof::{userEmail}::{seq}")
public class ProfileShape implements DocShape {

    static final List<String> MESSAGE_TEXT = messageText();
    static final List<String> TAGS = Arrays.asList(
            "urgent", "follow-up", "billing", "technical", "resolved", "spam", "vip",
            "escalated", "refund", "churn-risk", "upsell", "complaint", "praise",
            "callback", "no-response", "duplicate", "fraud-review", "gdpr", "reopened", "closed");

    @Path("userEmail")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.EMAIL)
    String userEmail;

    @Path("firstName")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.FIRST_NAME)
    String firstName;

    @Path("lastName")
    @Kind(SCALAR)
    @Identity(Identity.Attribute.LAST_NAME)
    String lastName;

    @Path("phoneNumbers")
    @Kind(ARRAY_OF_SCALAR)
    @Identity(Identity.Attribute.PHONE)
    @ArraySize(min = 1, max = 3)
    @Empty(prob = 0.08)
    List<String> phoneNumbers;

    // Mixed representations of the same instant: several string formats plus raw epoch millis.
    @Path("lastSeen")
    @Kind(SCALAR)
    @DateString(formats = {
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ssxxx",
            "yyyy-MM-dd HH:mm:ss",
            "dd/MM/yyyy HH:mm"},
            epochProb = 0.15)
    @Nullable(prob = 0.05)
    String lastSeen;

    // Most conversations are short; a small fraction are very long threads.
    @Path("msg")
    @Kind(ARRAY_OF_OBJECT)
    @ArraySize(min = 1, max = 4, largeProb = 0.02, largeMax = 150)
    List<Msg> msg;

    public static class Msg {
        @Path("ts")
        @Kind(SCALAR)
        @LongRange(min = 1767225600000L, max = 1798761600000L)
        long ts;

        @Path("text")
        @Kind(SCALAR)
        @ChoiceFrom("MESSAGE_TEXT")
        String text;

        @Path("note")
        @Kind(SCALAR)
        @Length(min = 12, max = 60, largeProb = 0.03, largeMax = 4096)
        @Optional(prob = 0.6)
        String note;

        @Path("tags")
        @Kind(ARRAY_OF_SCALAR)
        @ChoiceFrom("TAGS")
        @ArraySize(min = 0, max = 3)
        List<String> tags;
    }

    private static List<String> messageText() {
        List<String> pool = new ArrayList<String>(Arrays.asList(
                "Hello", "hello", "HELLO", "Thanks", "thanks!", "ok", "OK", "Please call back",
                "Order received", "Shipment delayed", "Refund processed", "Ticket closed",
                "Escalated to support", "Awaiting reply", "Can you confirm the delivery window?",
                "I have been charged twice for the same order and need this resolved today.",
                "The router keeps dropping connection every few hours since the upgrade.",
                "Følger opp på forrige melding", "请尽快回复", "Merci pour votre aide",
                "Se adjunta la factura correspondiente al mes pasado.",
                "No response yet — following up for the third time.",
                "Resolved, thank you for the quick turnaround!",
                "Line 1\nLine 2\nLine 3", "   leading and trailing   ", "",
                "Payment failed: card declined (code 51). Please update your billing details.",
                "Appointment rescheduled to next Tuesday between 9am and 12pm.",
                "Account closed at customer request; final bill issued."));
        for (int i = 0; i < 50; i++) {
            pool.add("Follow-up note #" + i + " regarding the open case.");
        }
        return pool;
    }
}
