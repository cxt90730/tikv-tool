package main

import (
	"encoding/hex"
	"gopkg.in/bufio.v1"
	"math"
	"strconv"
)

const NullVersion = "0"

type Table struct {
	GenKeyFunc func(args ...string) []byte
	Prefix     string
}

var TableMap = map[string]Table{
	"bucket": {
		Prefix: TableBucketPrefix,
	},
	"user": {
		Prefix: TableUserBucketPrefix,
	},
	"object": {
		Prefix: "",
	},
	"objectv": {
		Prefix: TableBucketPrefix,
	},
	"multipart": {
		Prefix: TableMultipartPrefix,
	},
	"part": {
		Prefix: TableObjectPartPrefix,
	},
	"cluster": {
		Prefix: TableClusterPrefix,
	},
	"gc": {
		Prefix: TableGcPrefix,
	},
	"freezer": {
		GenKeyFunc: nil,
	},
}

var (
	TableClusterPrefix       = "c"
	TableBucketPrefix        = "b"
	TableUserBucketPrefix    = "u"
	TableMultipartPrefix     = "m"
	TableObjectPartPrefix    = "p"
	TableLifeCyclePrefix     = "l"
	TableGcPrefix            = "g"
	TableFreezerPrefix       = "f"

	TableMinKeySuffix = ""
	TableMaxKeySuffix = string(0xFF)
	TableSeparator    = string(92) // "\"
)

func GenKey(args ...string) []byte {
	buf := bufio.NewBuffer([]byte{})
	for _, arg := range args {
		buf.WriteString(arg)
		buf.WriteString(TableSeparator)
	}
	key := buf.Bytes()

	return key[:len(key)-1]
}

// **Key**: b\{BucketName}
func genBucketKey(bucketName string) []byte {
	return GenKey(TableBucketPrefix, bucketName)
}

// **Key**: g\{PoolName}\{Fsid}\{ObjectId}
func genGcKey(poolName, fsid, objectId string) []byte {
	return GenKey(TableGcPrefix, poolName, fsid, objectId)
}

// **Key**: m\{BucketName}\{ObjectName}\{EncodedTime}
// UploadTime = MaxUint64 - multipart.InitialTime
// EncodedTime = hex.EncodeToString(BigEndian(UploadTime)）
func genMultipartKey(bucketName, objectName string, initialTime uint64) []byte {
	encodedTime := hex.EncodeToString(EncodeUint64(math.MaxUint64 - initialTime))
	return GenKey(TableMultipartPrefix, bucketName, objectName, encodedTime)
}

// **Key**: p\{BucketName}\{ObjectName}\{UploadId}\{EncodePartNumber}
// EncodePartNumber = hex.EncodeToString(BigEndian({PartNumber}))
func genObjectPartKey(bucketName, objectName, uploadId, partN string) []byte {
	partNumber, _ := strconv.ParseUint(partN, 10, 64)
	return GenKey(TableObjectPartPrefix, bucketName, objectName, uploadId, hex.EncodeToString(EncodeUint64(partNumber)))
}

// **Key**: u\{OwnerId}\{BucketName}
func genUserBucketKey(ownerId, bucketName string) []byte {
	return GenKey(TableUserBucketPrefix, ownerId, bucketName)
}

// **Key**: {BucketName}\{ObjectName}
// **Versioned Key**: v\{BucketName}\{ObjectName}\{Version}
// Version = hex.EncodeToString(BigEndian(MaxUint64 - object.LastModifiedTime.UnixNano()))
func genObjectKey(bucketName, objectName, version string) []byte {
	if version == NullVersion {
		return GenKey(bucketName, objectName)
	} else {
		return GenKey(bucketName, objectName, version)
	}
}

// Key: c\{PoolName}\{Fsid}\{Backend}
func genClusterKey(poolName, fsid, backend string) []byte {
	return GenKey(TableClusterPrefix, poolName, fsid, backend)
}
