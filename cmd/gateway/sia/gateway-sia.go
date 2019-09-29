/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sia

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/shomali11/util/xhashes"

	humanize "github.com/dustin/go-humanize"
	"github.com/fatih/color"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/sha256-simd"
)

const (
	siaBackend = "sia"
)

type fileCacheMeta struct {
	RedisClient string
}
type siaObjects struct {
	minio.GatewayUnsupported
	Address     string // Address and port of Sia Daemon.
	TempDir     string // Temporary storage location for file transfers.
	RootDir     string // Root directory to store files on Sia.
	password    string // Sia password for uploading content in authenticated manner.
	RedisClient *redis.Client
}

func init() {
	const siaGatewayTemplate = `NAME:
   {{.HelpName}} - {{.Usage}}
 
 USAGE:
   {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [SIA_DAEMON_ADDR]
 {{if .VisibleFlags}}
 FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}{{end}}
 ENVIRONMENT VARIABLES: (Default values in parenthesis)
   ACCESS:
	  MINIO_ACCESS_KEY: Custom access key (Do not reuse same access keys on all instances)
	  MINIO_SECRET_KEY: Custom secret key (Do not reuse same secret keys on all instances)
 
   BROWSER:
	  MINIO_BROWSER: To disable web browser access, set this value to "off".
 
   UPDATE:
	  MINIO_UPDATE: To turn off in-place upgrades, set this value to "off".
 
   SIA_TEMP_DIR:        The name of the local Sia temporary storage directory. (.sia_temp)
   SIA_API_PASSWORD:    API password for Sia daemon. (default is empty)
 
 EXAMPLES:
   1. Start minio gateway server for Sia backend.
	   $ {{.HelpName}}
 
 `

	minio.RegisterGatewayCommand(cli.Command{
		Name:               siaBackend,
		Usage:              "Sia Decentralized Cloud.",
		Action:             siaGatewayMain,
		CustomHelpTemplate: siaGatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway sia' command line.
func siaGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &Sia{host})
}

// Sia implements Gateway.
type Sia struct {
	host string // Sia daemon host address
}

// Name implements Gateway interface.
func (g *Sia) Name() string {
	return siaBackend
}

func (s *siaObjects) listenToRedisPubSub(channel string) {
	pubsub := s.RedisClient.PSubscribe(channel)
	defer pubsub.Close()

	for {
		msgi, err := pubsub.Receive()
		if err != nil {
			fmt.Println(nil, "PubSub error:", err.Error())
			return
		}
		switch msg := msgi.(type) {
		case *redis.Message:
			fmt.Println(nil, "Received", msg.Payload, "on channel", msg.Channel)
			if strings.Compare("expired", msg.Payload) == 0 {
				go func() {
					file := strings.Split(msg.Channel, ":")[1]
					fileHash := xhashes.SHA256(file)
					dstFile := path.Join(s.TempDir, fmt.Sprint(fileHash))
					os.Remove(dstFile)
				}()
			}
		default:
			fmt.Println(nil, "Got control message", msg)
		}
	}
}

// NewGatewayLayer returns Sia gateway layer, implements GatewayLayer interface to
// talk to Sia backend.
func (g *Sia) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	sia := &siaObjects{
		Address: g.host,
		// RootDir uses access key directly, provides partitioning for
		// concurrent users talking to same sia daemon.
		RootDir:  creds.AccessKey,
		TempDir:  os.Getenv("SIA_TEMP_DIR"),
		password: os.Getenv("SIA_API_PASSWORD"),
		RedisClient: redis.NewClient(&redis.Options{
			Addr:     "127.0.0.1:6379",
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
	}

	//go sia.listenToRedisPubSub("*")
	go sia.listenToRedisPubSub("__keyspace@0__:" + creds.AccessKey + "/*")

	var err error
	// Check if redis is available
	_, err = sia.RedisClient.Ping().Result()
	if err != nil {
		return nil, err
	}

	// If Address not provided on command line or ENV, default to:
	if sia.Address == "" {
		sia.Address = "127.0.0.1:9980"
	}

	// If local Sia temp directory not specified, default to:
	if sia.TempDir == "" {
		sia.TempDir = ".sia_temp"
	}

	sia.TempDir, err = filepath.Abs(sia.TempDir)
	if err != nil {
		return nil, err
	}

	// Create the temp directory with proper permissions.
	// Ignore error when dir already exists.
	if err = os.MkdirAll(sia.TempDir, 0700); err != nil {
		return nil, err
	}

	colorBlue := color.New(color.FgBlue).SprintfFunc()
	colorBold := color.New(color.Bold).SprintFunc()

	log.Println(colorBlue("\nSia Gateway Configuration:"))
	log.Println(colorBlue("  Sia Daemon API Address:") + colorBold(fmt.Sprintf(" %s\n", sia.Address)))
	log.Println(colorBlue("  Sia Temp Directory:") + colorBold(fmt.Sprintf(" %s\n", sia.TempDir)))
	return sia, nil
}

// Production - sia gateway is not ready for production use.
func (g *Sia) Production() bool {
	return false
}

// non2xx returns true for non-success HTTP status codes.
func non2xx(code int) bool {
	return code < 200 || code > 299
}

// decodeError returns the api.Error from a API response. This method should
// only be called if the response's status code is non-2xx. The error returned
// may not be of type api.Error in the event of an error un-marshalling the
// JSON.
type siaError struct {
	// Message describes the error in English. Typically it is set to
	// `err.Error()`. This field is required.
	Message string `json:"message"`
}

func (s siaError) Error() string {
	return s.Message
}

func decodeError(resp *http.Response) error {
	// Error is a type that is encoded as JSON and returned in an API response in
	// the event of an error. Only the Message field is required. More fields may
	// be added to this struct in the future for better error reporting.
	var apiErr siaError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		return err
	}
	return apiErr
}

// MethodNotSupported - returned if call returned error.
type MethodNotSupported struct {
	method string
}

func (s MethodNotSupported) Error() string {
	return fmt.Sprintf("API call not recognized: %s", s.method)
}

// apiGet wraps a GET request with a status code check, such that if the GET does
// not return 2xx, the error will be read and returned. The response body is
// not closed.
func apiGet(addr, call, apiPassword string) (*http.Response, error) {
	req, err := http.NewRequest("GET", "http://"+addr+call, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	if apiPassword != "" {
		req.SetBasicAuth("", apiPassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, MethodNotSupported{call}
	}
	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// apiPost wraps a POST request with a status code check, such that if the POST
// does not return 2xx, the error will be read and returned. The response body
// is not closed.
func apiPost(addr, call, vals, apiPassword string) (*http.Response, error) {
	req, err := http.NewRequest("POST", "http://"+addr+call, strings.NewReader(vals))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if apiPassword != "" {
		req.SetBasicAuth("", apiPassword)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, MethodNotSupported{call}
	}

	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return nil, err
	}
	return resp, nil
}

// post makes an API call and discards the response. An error is returned if
// the response status is not 2xx.
func post(addr, call, vals, apiPassword string) error {
	resp, err := apiPost(addr, call, vals, apiPassword)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// list makes a lists all the uploaded files, decodes the json response.
func list(addr string, apiPassword string, obj *renterFiles) error {
	resp, err := apiGet(addr, "/renter/files", apiPassword)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("Expecting a response, but API returned %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(obj)
}

// Get the info of given sia object, decodes the json response.
func getSiaObjectInfo(addr string, apiPassword string, siaObj string, obj *renterFileInfo) error {
	resp, err := apiGet(addr, "/renter/file/"+siaObj, apiPassword)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("Expecting a response, but API returned %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(obj)
}

// List the directories starting at the given directory
func listDirs(addr string, apiPassword string, startDirectory string, obj *renterDirectories) error {
	resp, err := apiGet(addr, "/renter/dir/"+startDirectory, apiPassword)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return fmt.Errorf("Expecting a response, but API returned %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(obj)
}

// get makes an API call and discards the response. An error is returned if the
// response status is not 2xx.
func get(addr, call, apiPassword string) error {
	resp, err := apiGet(addr, call, apiPassword)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (s *siaObjects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to Sia backend.
func (s *siaObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

// MakeBucket creates a new container on Sia backend.
func (s *siaObjects) MakeBucketWithLocation(ctx context.Context, bucket, location string) error {
	siaObj := path.Join(s.RootDir, bucket, "")

	// Create the directory by uploading the dummy file.
	if err := post(s.Address, "/renter/dir/"+siaObj, "action=create", s.password); err != nil {
		return err
	}

	return nil
}

// GetBucketInfo gets bucket metadata.
func (s *siaObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	fileHash := xhashes.SHA256(path.Join(s.RootDir, bucket, ""))
	dstFile := path.Join(s.TempDir, fmt.Sprint(fileHash))
	var siaObj = path.Join(s.RootDir, bucket, fileHash)

	defer os.Remove(dstFile)

	if err := get(s.Address, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(dstFile), s.password); err != nil {
		return bi, err
	}
	return minio.BucketInfo{Name: bucket}, nil
}

// ListBuckets will detect and return existing buckets on Sia.
func (s *siaObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	sObjs, serr := s.listRenterDirectories(s.RootDir)
	if serr != nil {
		return buckets, serr
	}

	prefix := s.RootDir + "/"
	for _, sObj := range sObjs {
		if strings.HasPrefix(sObj.SiaPath, prefix) {
			bucketName := strings.TrimPrefix(sObj.SiaPath, prefix)
			idx := strings.Index(bucketName, "/")
			// Check if there is no more path segments, this is a bucket (e. g. minio-2018/test1 will result in test1 which is the bucket)
			if idx == -1 {
				buckets = append(buckets, minio.BucketInfo{
					Name: bucketName,
					Created: func() time.Time {
						value, _ := time.Parse(time.RFC3339Nano, sObj.MostRecentModTime)
						return value
					}(),
				})
			}
		}
	}

	return buckets, nil
}

// DeleteBucket deletes a bucket on Sia.
func (s *siaObjects) DeleteBucket(ctx context.Context, bucket string) error {
	var siaObj = path.Join(s.RootDir, bucket)

	return post(s.Address, "/renter/dir/"+siaObj, "action=delete", s.password)
}

func (s *siaObjects) ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	siaObjs, siaErr := s.listRenterFiles(bucket)
	if siaErr != nil {
		return loi, siaErr
	}

	loi.IsTruncated = false
	loi.NextMarker = ""

	root := s.RootDir + "/"

	sha256sum := sha256.Sum256([]byte(path.Join(root, bucket)))
	// FIXME(harsha) - No paginated output supported for Sia backend right now, only prefix
	// based filtering. Once list renter files API supports paginated output we can support
	// paginated results here as well - until then Listing is an expensive operation.
	for _, sObj := range siaObjs {
		name := strings.TrimPrefix(sObj.SiaPath, path.Join(root, bucket)+"/")
		// Skip the file created specially when bucket was created.
		if name == hex.EncodeToString(sha256sum[:]) {
			continue
		}
		if strings.HasPrefix(name, prefix) {
			loi.Objects = append(loi.Objects, minio.ObjectInfo{
				Bucket: bucket,
				Name:   name,
				Size:   int64(sObj.Filesize),
				IsDir:  false,
				ModTime: func(sObj siaObjectFileInfo) time.Time {
					value, _ := time.Parse(time.RFC3339Nano, sObj.ModTime)
					return value
				}(sObj),
			})
		}
	}
	return loi, nil
}

func (s *siaObjects) GetObject(ctx context.Context, bucket string, object string, startOffset int64, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	fileHash := xhashes.SHA256(path.Join(s.RootDir, bucket, object))
	dstFile := path.Join(s.TempDir, fmt.Sprint(fileHash))

	reader, err := os.Open(dstFile)
	// If file is available but size is 0 download is running
	if err == nil {
		stat, _ := reader.Stat()
		if stat.Size() == 0 {
			return nil //Just return do nothing
		}
	}

	if err != nil { // There was an error opening the file so download it
		redisErr := s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 0, 0).Err()
		if redisErr != nil {
			panic(redisErr)
		}
		var siaObj = path.Join(s.RootDir, bucket, object)
		if err := get(s.Address, "/renter/download/"+siaObj+"?destination="+url.QueryEscape(dstFile), s.password); err != nil {
			s.RedisClient.Del(path.Join(s.RootDir, bucket, object))
			return err
		}
		// set key to 1 indicating it is safe to delete file from cache
		redisErr = s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 1, time.Hour*24*7).Err()
		if redisErr != nil {
			panic(redisErr)
		}
	} else {
		// File is available in cache, update file cache meta
		val, err := s.RedisClient.Get(path.Join(s.RootDir, bucket, object)).Result()
		if err == redis.Nil {
			redisErr := s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 1, time.Hour*24*7).Err()
			if redisErr != nil {
				panic(redisErr)
			}
		} else {
			intVal, _ := strconv.Atoi(val)
			if intVal != 0 {
				// set key to 1 indicating it is safe to delete file from cache
				redisErr := s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 1, time.Hour*24*7).Err()
				if redisErr != nil {
					panic(redisErr)
				}
			} else {
				// set key to 0 indicating it is not safe to delete file from cache
				redisErr := s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 0, 0).Err()
				if redisErr != nil {
					panic(redisErr)
				}
			}
		}
	}

	reader, err = os.Open(dstFile)
	if err != nil {
		return err
	}
	defer reader.Close()
	st, err := reader.Stat()
	if err != nil {
		return err
	}
	size := st.Size()
	if _, err = reader.Seek(startOffset, os.SEEK_SET); err != nil {
		return err
	}

	// For negative length we read everything.
	if length < 0 {
		length = size - startOffset
	}

	bufSize := int64(1 * humanize.MiByte)
	if bufSize > length {
		bufSize = length
	}

	// Reply back invalid range if the input offset and length fall out of range.
	if startOffset > size || startOffset+length > size {
		return minio.InvalidRange{
			OffsetBegin:  startOffset,
			OffsetEnd:    length,
			ResourceSize: size,
		}
	}

	// Allocate a staging buffer.
	buf := make([]byte, int(bufSize))

	_, err = io.CopyBuffer(writer, io.LimitReader(reader, length), buf)

	return err
}

func (s *siaObjects) getSiaObjectInfo(bucket, object string) (siaObjectFileInfo, error) {
	// This is not needed probably but supports minio s3 behavior where / as a suffix means list directory.
	if strings.HasSuffix(object, "/") {
		return siaObjectFileInfo{}, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	siaObj := path.Join(s.RootDir, bucket, object)
	s.getRenterFileInfo(bucket, siaObj)

	return siaObjectFileInfo{}, minio.ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

// findSiaObject retrieves the siaObjectInfo for the Sia object with the given
// Sia path name.
func (s *siaObjects) findSiaObject(bucket, object string) (siaObjectFileInfo, error) {
	// This is not needed probably but supports minio s3 behavior where / as a suffix means list directory.
	if strings.HasSuffix(object, "/") {
		return siaObjectFileInfo{}, minio.ObjectNotFound{
			Bucket: bucket,
			Object: object,
		}
	}

	siaPath := path.Join(s.RootDir, bucket, object)

	sObjs, err := s.listRenterFiles("")
	if err != nil {
		return siaObjectFileInfo{}, err
	}

	for _, sObj := range sObjs {
		if sObj.SiaPath == siaPath {
			return sObj, nil
		}
	}

	return siaObjectFileInfo{}, minio.ObjectNotFound{
		Bucket: bucket,
		Object: object,
	}
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (s *siaObjects) GetObjectInfo(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	so, err := s.findSiaObject(bucket, object)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	// Metadata about sia objects is just quite minimal. Sia only provides file size.
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		Size:    int64(so.Filesize),
		IsDir:   false,
	}, nil
}

// GetObjectNInfo - returns object info and locked object ReadCloser
func (s *siaObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = s.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		err := s.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(err)
	}()
	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts.CheckCopyPrecondFn, pipeCloser)
}

// PutObject creates a new object with the incoming data,
func (s *siaObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	pr, pw := io.Pipe()
	bufferin := bufio.NewReader(r.Reader)
	siaObj := path.Join(s.RootDir, bucket, object)

	go func(objReader *bufio.Reader) {
		// close the writer, so the reader knows there's no more data
		defer pw.Close()

		// write data to the pipe writer
		bufferin.WriteTo(pw)
	}(bufferin)

	req, err := http.NewRequest("POST", "http://"+s.Address+"/renter/uploadstream/"+siaObj, pr)
	if err != nil {
		return objInfo, err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/octet-stream")
	if s.password != "" {
		req.SetBasicAuth("", s.password)
	}
	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return objInfo, err
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return objInfo, MethodNotSupported{"/renter/uploadstream/" + siaObj}
	}

	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return objInfo, err
	}

	// We want to make sure once the uploading client receives request successfull the file is available from Sia.
	// So let's wait for the file to become available after upload stream has finished which should happen
	// within a view moments on a Sia node with good internet bandwidth. With further development of the Sia
	// technology this should be even faster.
	for {
		siaObjInfo, _ := s.getRenterFileInfo(bucket, siaObj)
		// If the sia path is empty probably upload was canceled.
		if siaObjInfo.FileInfo.SiaPath == "" {
			break
		}
		// If the file is available on the sia network stop waiting.
		if siaObjInfo.FileInfo.Available == true {
			break
		}

		time.Sleep(time.Second * 3)
	}

	return minio.ObjectInfo{
		Name:    object,
		Bucket:  bucket,
		ModTime: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		Size:    r.Reader.Size(),
		ETag:    minio.GenETag(),
	}, nil
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObject(ctx context.Context, bucket string, object string) error {
	fileHash := xhashes.SHA256(path.Join(s.RootDir, bucket, object))
	objectFile := path.Join(s.TempDir, fmt.Sprint(fileHash))
	defer os.Remove(objectFile)

	redisErr := s.RedisClient.Del(path.Join(s.RootDir, bucket, object)).Err()
	if redisErr != nil {
		panic(redisErr)
	}

	// Tell Sia daemon to delete the object
	var siaObj = path.Join(s.RootDir, bucket, object)
	return post(s.Address, "/renter/delete/"+siaObj, "", s.password)
}

// DeleteObject deletes a blob in bucket
func (s *siaObjects) DeleteObjects(ctx context.Context, bucket string, objects []string) ([]error, error) {
	errs := make([]error, len(objects))

	// TODO Implement

	return errs, nil
}

// siaObjectFileInfo represents object info stored on files on Sia
type siaObjectFileInfo struct {
	SiaPath        string  `json:"siapath"`
	LocalPath      string  `json:"localpath"`
	Filesize       uint64  `json:"filesize"`
	Available      bool    `json:"available"`
	Renewing       bool    `json:"renewing"`
	Redundancy     float64 `json:"redundancy"`
	UploadProgress float64 `json:"uploadprogress"`
	ModTime        string  `json:"modtime"`
}

// siaObjectDirectoryInfo represents object info stored on directories on Sia
type siaObjectDirectoryInfo struct {
	SiaPath           string `json:"siapath"`
	MostRecentModTime string `json:"mostrecentmodtime"`
}

type renterFiles struct {
	Files []siaObjectFileInfo `json:"files"`
}

type renterFileInfo struct {
	FileInfo siaObjectFileInfo `json:"file"`
}

type renterDirectories struct {
	Directories []siaObjectDirectoryInfo `json:"directories"`
}

// listRenterFiles will return a list of existing objects in the bucket provided
func (s *siaObjects) listRenterFiles(bucket string) (siaObjs []siaObjectFileInfo, err error) {
	// Get list of all renter files
	var rf renterFiles
	if err = list(s.Address, s.password, &rf); err != nil {
		return siaObjs, err
	}

	var prefix string
	root := s.RootDir + "/"
	if bucket == "" {
		prefix = root
	} else {
		prefix = root + bucket + "/"
	}

	for _, f := range rf.Files {
		if strings.HasPrefix(f.SiaPath, prefix) {
			siaObjs = append(siaObjs, f)
		}
	}

	return siaObjs, nil
}

// getRenterFileInfo will return a info of an existing object in the bucket provided
func (s *siaObjects) getRenterFileInfo(bucket, siaObj string) (siaObjFileInfo renterFileInfo, err error) {
	if err = getSiaObjectInfo(s.Address, s.password, siaObj, &siaObjFileInfo); err != nil {
		return siaObjFileInfo, err
	}

	return siaObjFileInfo, nil
}

// listRenterDirectories will return a list of existing directories starting from the directory provided
func (s *siaObjects) listRenterDirectories(startDirectory string) (siaObjs []siaObjectDirectoryInfo, err error) {
	// Get list of all renter directories from startDirectory
	var rd renterDirectories
	if err = listDirs(s.Address, s.password, startDirectory, &rd); err != nil {
		return siaObjs, err
	}

	for _, f := range rd.Directories {
		siaObjs = append(siaObjs, f)
	}

	return siaObjs, nil
}

// updateFileCacheMetaAfterUploadedToSia checks the status of a Sia file upload
// until it reaches 100% upload progress, then deletes the local temp copy from
// the filesystem.
func (s *siaObjects) updateFileCacheMetaAfterUploadedToSia(tempFile string, bucket, object string) {
	var soi siaObjectFileInfo
	// Wait until 100% upload instead of 1x redundancy because if we delete
	// after 1x redundancy, the user has to pay the cost of other hosts
	// redistributing the file.

	for soi.UploadProgress < 100.0 {
		var err error
		soi, err = s.findSiaObject(bucket, object)
		if err != nil {
			logger.Info("Unable to find file uploaded to Sia path %s/%s", bucket, object)
			break
		}

		// Sleep between each check so that we're not hammering
		// the Sia daemon with requests.
		time.Sleep(15 * time.Second)
	}

	// set key to 1 indicating it is uploaded and it is safe o delete it from cache
	// File is available in cache, update or create file cache meta info
	_, err := s.RedisClient.Get(path.Join(s.RootDir, bucket, object)).Result()
	// Check if key exists if not upload probably was canceled. Do nothing in this case.
	if err != redis.Nil || err == nil {
		redisErr := s.RedisClient.Set(path.Join(s.RootDir, bucket, object), 1, time.Hour*24*7).Err()
		if redisErr != nil {
			panic(redisErr)
		}
	}
}
