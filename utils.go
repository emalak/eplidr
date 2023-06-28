package eplidr

import (
	"encoding/json"
	"fmt"
)

func JSON(object any) (string, error) {
	bytes, err := json.Marshal(object)
	return string(bytes), err
}

func JSONList[T any](list []T) string {
	return fmt.Sprintf("[%s]", PlainList(list))
}

func PlainList[T any](list []T) string {
	result := ""
	for _, item := range list {
		result += fmt.Sprintf(`%v,`, item)
	}
	if len(result) > 0 {
		result = result[:len(result)-1]
	}
	return result
}
func PlainListNoSep[T any](list []T) string {
	result := ""
	for _, item := range list {
		result += fmt.Sprintf(`%v`, item)
	}
	if len(result) > 0 {
		result = result[:len(result)-1]
	}
	return result
}
