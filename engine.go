package boltsql

import "encoding/binary"

type value interface {
	Serialize() []byte
}

type (
	nullValue    struct{}
	boolValue    bool
	stringValue  []byte
	integerValue int64
)

func (v nullValue) Serialize() []byte {
	return []byte{0}
}

func (v boolValue) Serialize() []byte {
	if v {
		return []byte{1, 1}
	}
	return []byte{1, 0}
}

func (v stringValue) Serialize() []byte {
	result := make([]byte, len(v)+1)
	result[0] = 2
	copy(result[1:], v)
	return result
}

func (v integerValue) Serialize() []byte {
	result := make([]byte, 9)
	result[0] = 3
	binary.LittleEndian.PutUint64(result[1:], uint64(v))
	return result
}

func deserialize(data []byte) value {
	if len(data) == 0 {
		panic("Empty data")
	}

	switch data[0] {
	case 0:
		return nullValue{}
	case 1:
		if len(data) < 2 {
			panic("Invalid boolean data")
		}
		return boolValue(data[1] == 1)
	case 2:
		return stringValue(data[1:])
	case 3:
		if len(data) < 9 {
			panic("Invalid integer data")
		}
		return integerValue(int64(binary.LittleEndian.Uint64(data[1:])))
	default:
		panic("Invalid data format")
	}
}

type row struct {
	cells  [][]byte
	fields []string
}
