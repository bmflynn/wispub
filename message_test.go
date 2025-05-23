package main

import "testing"

func TestMimeType(t *testing.T) {
	cases := []struct {
		path     string
		expected string
	}{
		{"foo.bufr", "application/bufr"},
		{"foo.bufr.bin", "application/bufr"},
		{"foo.grib", "application/grib"},
		{"foo.grib.bin", "application/grib"},
		{"foo", "application/octet-stream"},
	}
	for _, test := range cases {
		if typ := mimeTypeByExtension(test.path); typ != test.expected {
			t.Errorf("expected %v for %v, got '%v'", test.expected, test.path, typ)
		}
	}
}
