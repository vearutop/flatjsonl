package flatjsonl

import "testing"

func TestTypeUpdate_JSONDominatesScalar(t *testing.T) {
	cases := []struct {
		name string
		a    Type
		b    Type
	}{
		{name: "float to json", a: TypeFloat, b: TypeJSON},
		{name: "json to float", a: TypeJSON, b: TypeFloat},
		{name: "int to json", a: TypeInt, b: TypeJSON},
		{name: "json to int", a: TypeJSON, b: TypeInt},
		{name: "string to json", a: TypeString, b: TypeJSON},
		{name: "json to string", a: TypeJSON, b: TypeString},
		{name: "bool to json", a: TypeBool, b: TypeJSON},
		{name: "json to bool", a: TypeJSON, b: TypeBool},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Update(tc.b); got != TypeJSON {
				t.Fatalf("expected %s + %s to be %s, got %s", tc.a, tc.b, TypeJSON, got)
			}
		})
	}
}
