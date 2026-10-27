package avro_test

import "github.com/iskorotkov/avro/v3"

func ConfigTeardown() {
	// Reset the caches
	avro.DefaultConfig = avro.Config{}.Freeze()
}
