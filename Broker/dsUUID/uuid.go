package dsuuid

import (
	"fmt"
	"math/rand"
	"time"
)

type UUID [16]byte

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

func NewUUID() UUID {
	var uuid UUID

	hi := rng.Int63()
	lo := rng.Int63()
	for i := range 8 {
		uuid[i]   = byte(hi >> (i * 8))
		uuid[i+8] = byte(lo >> (i * 8))
	}

	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80

	return uuid
}

func (u UUID) String() string {
	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		u[0:4],
		u[4:6],
		u[6:8],
		u[8:10],
		u[10:],
	)
}
