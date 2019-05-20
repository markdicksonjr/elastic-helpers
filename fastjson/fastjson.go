package util

import (
	"github.com/pkg/errors"
	"github.com/valyala/fastjson"
	"strconv"
	"strings"
)

// get dot-prop value from a fastjson value
func GetSubValue(value *fastjson.Value, key string) *fastjson.Value {
	if value == nil {
		return nil
	}

	if !strings.Contains(key, ".") {
		return value.Get(key)
	}

	pathParts := strings.Split(key, ".")
	return GetSubValue(value.Get(pathParts[0]), strings.Join(pathParts[1:], "."))
}

// get dot-prop value from a fastjson object
func GetValueFromObject(value *fastjson.Object, key string) *fastjson.Value {
	if value == nil {
		return nil
	}

	if !strings.Contains(key, ".") {
		return value.Get(key)
	}

	pathParts := strings.Split(key, ".")
	return GetSubValue(value.Get(pathParts[0]), strings.Join(pathParts[1:], "."))
}

// gets a string from the given dot-path - guaranteed to be non-nil
func GetSafeStringFromValue(value *fastjson.Value, key string, defaultValue string) string {
	return *GetStringFromValue(value, key, &defaultValue)
}

// gets a string from the given dot-path
func GetStringFromValue(value *fastjson.Value, key string, defaultValue *string) *string {
	subValue := GetSubValue(value, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToString(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

// gets a string from the given dot-path - guaranteed to be non-nil
func GetSafeStringFromObject(object *fastjson.Object, key string, defaultValue string) string {
	return *GetStringFromObject(object, key, &defaultValue)
}

// gets a string from the given dot-path
func GetStringFromObject(object *fastjson.Object, key string, defaultValue *string) *string {
	subValue := GetValueFromObject(object, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToString(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

// gets an int from the given dot-path
func GetIntFromObject(object *fastjson.Object, key string, defaultValue *int) *int {
	subValue := GetValueFromObject(object, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToInt(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

// gets an int from the given dot-path
func GetIntFromValue(value *fastjson.Value, key string, defaultValue *int) *int {
	subValue := GetSubValue(value, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToInt(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

// gets a float from the given dot-path
func GetFloatFromObject(object *fastjson.Object, key string, defaultValue *float64) *float64 {
	subValue := GetValueFromObject(object, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToFloat(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

// gets a float from the given dot-path
func GetFloatFromValue(value *fastjson.Value, key string, defaultValue *float64) *float64 {
	subValue := GetSubValue(value, key)
	if subValue == nil {
		return defaultValue
	}

	coerced, err := CoerceToFloat(subValue)
	if err == nil {
		return &coerced
	}

	return defaultValue
}

func CoerceToInt(value *fastjson.Value) (int, error) {
	valueBytes, err := value.Int()
	if err == nil {
		return valueBytes, nil
	}

	partTypeAsString, err := value.StringBytes()
	if err == nil {
		asI, err := strconv.Atoi(string(partTypeAsString))
		if err == nil {
			return asI, nil
		}
	}

	partTypeAsFloat, err := value.Float64()
	if err == nil {
		return int(partTypeAsFloat), nil
	}

	return 0, errors.New("could not coerce field to int")
}

func CoerceToFloat(value *fastjson.Value) (float64, error) {
	asFloat, err := value.Float64()
	if err == nil {
		return asFloat, nil
	}

	asStringBytes, err := value.StringBytes()
	if err == nil {
		asI, err := strconv.ParseFloat(string(asStringBytes), 64)
		if err == nil {
			return asI, nil
		}
	}

	asInt, err := value.Int()
	if err == nil {
		return float64(asInt), nil
	}

	return 0, errors.New("could not coerce field to float")
}

func CoerceToString(value *fastjson.Value) (string, error) {
	valueBytes, err := value.StringBytes()
	if err == nil {
		return string(valueBytes), nil
	}

	partTypeAsInt, err := value.Int()
	if err == nil {
		return strconv.Itoa(int(partTypeAsInt)), nil
	}

	partTypeAsFloat, err := value.Float64()
	if err == nil {
		return strconv.FormatFloat(partTypeAsFloat, 'f', -1, 64), nil
	}

	return "", errors.New("could not coerce field to string")
}
