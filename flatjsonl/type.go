package flatjsonl

// Type is a scalar type.
type Type string

// Type enumeration.
const (
	TypeString = Type("string")
	TypeInt    = Type("int")
	TypeFloat  = Type("float")
	TypeBool   = Type("bool")
	TypeNull   = Type("null")
	TypeAbsent = Type("")
)

// Update merges original type with updated.
func (t Type) Update(u Type) Type {
	// Undefined type is replaced by update.
	if t == "" {
		return u
	}

	// Same type is not updated.
	if t == u {
		return t
	}

	// String replaces any type.
	if u == TypeString || t == TypeString {
		return TypeString
	}

	// Bool and non-bool make unconstrained type: string.
	if t == TypeBool && u != TypeBool {
		return TypeString
	}

	// Float overrides Int.
	if t == TypeInt && u == TypeFloat {
		return TypeFloat
	}

	if t == TypeFloat && u == TypeInt {
		return TypeFloat
	}

	panic("don't know how to update " + t + " with " + u)
}
