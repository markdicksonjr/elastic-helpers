package formats

import "testing"

func TestEncodePropertyName(t *testing.T) {
	propRes := EncodePropertyName("prop.X.")
	if propRes != "prop.X" {
		t.Fatal("trailing period was not removed")
	}

	propReses := EncodePropertyName("prop.X..")
	if propReses != "prop.X" {
		t.Fatal("trailing periods were not removed")
	}

	propParents := EncodePropertyName("Product (FG)")
	if propParents != "Product _FG_" {
		t.Fatal("parentheses were not removed")
	}

	propTrailingSpace := EncodePropertyName("Trailing ")
	if propTrailingSpace != "Trailing" {
		t.Fatal("trailing space was not removed")
	}

	propColon := EncodePropertyName("fg:123")
	if propColon != "fg_123" {
		t.Fatal("color was not removed")
	}
}
