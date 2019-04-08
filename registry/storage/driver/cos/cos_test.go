package cos

import (
	"context"
	"github.com/sirupsen/logrus"
	"testing"
	"math/rand"
	"bytes"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

var cosDriverConstructor func() (*Driver, error)

var skipCheck func() string

func init() {
	
	secretID := "AKIDV7hP9kFZjoo54vgxDIGWdbRGzXNCHRuD"
	secretKey := "JSPDFCB0eAVfdcXJyWu7HTYfcYL0W4Tr"
	bucket := "registry-1256271634"
	region := "ap-guangzhou"
	secure := false

	cosDriverConstructor = func() (*Driver, error) {
		parameters := DriverParameters{
			SecretID:  secretID,
			SecretKey: secretKey,
			Bucket:    bucket,
			Region:    region,
			Secure:    secure,
		}
		return New(parameters)
	}
	// Skip cos storage driver tests if environment variable parameters are not provided
	skipCheck = func() string {
		if secretID == "" || secretKey == "" || bucket == "" || region == "" {
			return "Must set ALIYUN_ACCESS_KEY_ID, ALIYUN_ACCESS_KEY_SECRET, OSS_REGION, OSS_BUCKET, and OSS_ENCRYPT to run OSS tests"
		}
		return ""
	}
	testsuites.RegisterSuite(func() (storagedriver.StorageDriver, error) {
		return cosDriverConstructor()
	}, skipCheck)
}

func TestEmptyRootList(t *testing.T) {
	logrus.Infof("正在执行 TestEmptyRootList")
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	rootedDriver, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("unexpected error creating rooted driver: %v", err)
	}

	filename := "/test"
	contents := []byte("contents")
	ctx := context.Background()
	err = rootedDriver.PutContent(ctx, filename, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
		return
	}
	defer rootedDriver.Delete(ctx, filename)

	keys, err := rootedDriver.List(ctx, "/")
	for _, path := range keys {
		if !storagedriver.PathRegexp.MatchString(path) {
			t.Fatalf("unexpected string in path: %q != %q", path, storagedriver.PathRegexp)
		}
	}
	_, err = rootedDriver.Stat(ctx, filename)
	if err != nil {
		t.Fatalf("unexpected error Stat: %v", err)
		return
	}
	var contentData []byte
	contentData, err= rootedDriver.GetContent(ctx, filename)
    if err != nil {
		t.Fatalf("unexpected error GetContent: %v", err)
		return 
	}
	logrus.Debugf("contentData: %d", len(contentData))

	var url string
	url, err = rootedDriver.URLFor(ctx, filename, make(map[string]interface{}))
	if err != nil {
		t.Fatalf("unexpected error URLFor: %v", err)
	}
    logrus.Debugf("contentData: %d", url)
}

func TestMoveWithMultipartCopy(t *testing.T) {
	if skipCheck() != "" {
		t.Skip(skipCheck())
	}

	d, err := cosDriverConstructor()
	if err != nil {
		t.Fatalf("unexpected error creating driver: %v", err)
	}

	ctx := context.Background()
	sourcePath := "/source"
	destPath := "/dest"

	defer d.Delete(ctx, sourcePath)
	defer d.Delete(ctx, destPath)

	// An object larger than d's MultipartCopyThresholdSize will cause d.Move() to perform a multipart copy.
	multipartCopyThresholdSize := 128 << 20 
	contents := make([]byte, 2*multipartCopyThresholdSize)
	rand.Read(contents)

	err = d.PutContent(ctx, sourcePath, contents)
	if err != nil {
		t.Fatalf("unexpected error creating content: %v", err)
	}

	err = d.Move(ctx, sourcePath, destPath)
	if err != nil {
		t.Fatalf("unexpected error moving file: %v", err)
	}

	received, err := d.GetContent(ctx, destPath)
	if err != nil {
		t.Fatalf("unexpected error getting content: %v", err)
	}
	if !bytes.Equal(contents, received) {
		t.Fatal("content differs")
	}

	_, err = d.GetContent(ctx, sourcePath)
	switch err.(type) {
	case storagedriver.PathNotFoundError:
	default:
		t.Fatalf("unexpected error getting content: %v", err)
	}
}
