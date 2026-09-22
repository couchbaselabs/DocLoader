package utils.val.shapes.engine;

/**
 * The closed set of structural patterns a shape field may declare.
 * Adding a value here requires adding a matching handler in ShapeWalker.
 */
public enum FieldKind {
    SCALAR,
    OBJECT,
    ARRAY_OF_SCALAR,
    ARRAY_OF_OBJECT,
    ARRAY_OF_ARRAY_OF_SCALAR,
    ARRAY_OF_ARRAY_OF_OBJECT,
    RECURSIVE_SELF,
    /** Same path, different scalar type across documents. Candidates come from @OneOf. */
    MIXED_TYPE,
    /** Object whose keys are data rather than schema, driving OBJECT_PAIRS and PAIRS(SELF). */
    OBJECT_DYNAMIC_KEYS
}
