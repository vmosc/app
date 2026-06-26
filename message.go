package app

import (
	"app/kernel/log"
	"encoding/binary"
	"io"
	"reflect"
	"sync"
)

// Message 消息结构体。
type Message struct {
	ID        string            `json:"id"`
	Version   uint8             `json:"version"`
	Timestamp int64             `json:"timestamp"`
	Method    string            `json:"method"`
	Metadata  map[string]string `json:"metadata"`
	Body      []byte            `json:"body"`
}

type fieldInfo struct {
	Name      string
	Type      reflect.Kind
	Index     int
	Marshal   func(v reflect.Value, buf *[]byte) error
	Unmarshal func(data []byte, pos *int, v reflect.Value) error
}

var (
	messageFields []fieldInfo
	fieldsMu      sync.RWMutex
)

// refreshMessageFields 刷新字段编解码信息（通过反射）。
func refreshMessageFields() {
	fieldsMu.Lock()
	defer fieldsMu.Unlock()

	t := reflect.TypeOf(Message{})
	fields := make([]fieldInfo, 0, t.NumField())

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		fi := fieldInfo{Name: field.Name, Type: field.Type.Kind(), Index: i}
		switch field.Type.Kind() {
		case reflect.String:
			fi.Marshal, fi.Unmarshal = marshalString, unmarshalString
		case reflect.Uint8:
			fi.Marshal, fi.Unmarshal = marshalUint8, unmarshalUint8
		case reflect.Int64:
			fi.Marshal, fi.Unmarshal = marshalInt64, unmarshalInt64
		case reflect.Map:
			if field.Type.Key().Kind() == reflect.String && field.Type.Elem().Kind() == reflect.String {
				fi.Marshal, fi.Unmarshal = marshalStringMap, unmarshalStringMap
			}
		case reflect.Slice:
			if field.Type.Elem().Kind() == reflect.Uint8 {
				fi.Marshal, fi.Unmarshal = marshalBytes, unmarshalBytes
			}
		}
		fields = append(fields, fi)
	}
	messageFields = fields
}

func getMessageFields() []fieldInfo {
	fieldsMu.RLock()
	defer fieldsMu.RUnlock()
	return messageFields
}

// RefreshMessageCodec 刷新编解码缓存（供业务层调用）。
func RefreshMessageCodec() {
	refreshMessageFields()
}

func init() {
	refreshMessageFields()
}

// MarshalBinary 将 Message 编码为二进制数据。
func (m *Message) MarshalBinary() ([]byte, error) {
	v := reflect.ValueOf(m).Elem()
	data := GetBuffer(0)
	defer PutBuffer(data)
	data = data[:0]
	for _, fi := range getMessageFields() {
		if fi.Marshal != nil {
			if err := fi.Marshal(v.Field(fi.Index), &data); err != nil {
				log.Error("message: marshal field failed", "field", fi.Name, "msg_id", m.ID, "err", err)
				return nil, err
			}
		}
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

// UnmarshalBinary 从二进制数据解码 Message。
func (m *Message) UnmarshalBinary(data []byte) error {
	v := reflect.ValueOf(m).Elem()
	pos := 0
	for _, fi := range getMessageFields() {
		if fi.Unmarshal != nil {
			if err := fi.Unmarshal(data, &pos, v.Field(fi.Index)); err != nil {
				log.Error("message: decode message failed", "err", err, "field", fi.Name, "msg_id", m.ID)
				return err
			}
		}
	}
	if pos < len(data) {
		log.Warn("message: extra data after unmarshal", "msg_id", m.ID, "expected", pos, "actual", len(data))
	}
	return nil
}

// 字段编解码辅助函数
func marshalString(v reflect.Value, buf *[]byte) error {
	s := v.String()
	*buf = append(*buf, encodeString(s)...)
	return nil
}

func unmarshalString(data []byte, pos *int, v reflect.Value) error {
	s, err := decodeString(data, pos)
	if err != nil {
		return err
	}
	v.SetString(s)
	return nil
}

func marshalUint8(v reflect.Value, buf *[]byte) error {
	val := uint8(v.Uint())
	*buf = append(*buf, val)
	return nil
}

func unmarshalUint8(data []byte, pos *int, v reflect.Value) error {
	if *pos >= len(data) {
		return io.EOF
	}
	val := data[*pos]
	*pos++
	v.SetUint(uint64(val))
	return nil
}

func marshalInt64(v reflect.Value, buf *[]byte) error {
	val := v.Int()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(val))
	*buf = append(*buf, b...)
	return nil
}

func unmarshalInt64(data []byte, pos *int, v reflect.Value) error {
	if *pos+8 > len(data) {
		return io.EOF
	}
	val := int64(binary.BigEndian.Uint64(data[*pos : *pos+8]))
	*pos += 8
	v.SetInt(val)
	return nil
}

func marshalStringMap(v reflect.Value, buf *[]byte) error {
	m := v.Interface().(map[string]string)
	length := uint32(len(m))
	*buf = append(*buf, encodeUint32(length)...)
	for k, val := range m {
		*buf = append(*buf, encodeString(k)...)
		*buf = append(*buf, encodeString(val)...)
	}
	return nil
}

func unmarshalStringMap(data []byte, pos *int, v reflect.Value) error {
	length, err := decodeUint32(data, pos)
	if err != nil {
		return err
	}
	m := make(map[string]string, length)
	for i := uint32(0); i < length; i++ {
		key, err := decodeString(data, pos)
		if err != nil {
			return err
		}
		val, err := decodeString(data, pos)
		if err != nil {
			return err
		}
		m[key] = val
	}
	v.Set(reflect.ValueOf(m))
	return nil
}

func marshalBytes(v reflect.Value, buf *[]byte) error {
	b := v.Bytes()
	length := uint32(len(b))
	*buf = append(*buf, encodeUint32(length)...)
	*buf = append(*buf, b...)
	return nil
}

func unmarshalBytes(data []byte, pos *int, v reflect.Value) error {
	length, err := decodeUint32(data, pos)
	if err != nil {
		return err
	}
	if *pos+int(length) > len(data) {
		return io.EOF
	}
	b := data[*pos : *pos+int(length)]
	*pos += int(length)
	v.SetBytes(b)
	return nil
}

// 底层编码辅助函数
func encodeUint32(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

func encodeString(s string) []byte {
	b := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(b[:4], uint32(len(s)))
	copy(b[4:], s)
	return b
}

func decodeUint32(data []byte, pos *int) (uint32, error) {
	if *pos+4 > len(data) {
		return 0, io.EOF
	}
	val := binary.BigEndian.Uint32(data[*pos : *pos+4])
	*pos += 4
	return val, nil
}

func decodeString(data []byte, pos *int) (string, error) {
	length, err := decodeUint32(data, pos)
	if err != nil {
		return "", err
	}
	if *pos+int(length) > len(data) {
		return "", io.EOF
	}
	s := string(data[*pos : *pos+int(length)])
	*pos += int(length)
	return s, nil
}
