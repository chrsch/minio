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
	"os"
	"path"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/policy"
	"github.com/minio/minio/pkg/policy/condition"
	"github.com/minio/sha256-simd"
)

const (
	siaBackend = "sia"
)

type siaObjects struct {
	minio.GatewayUnsupported
	Address  string // Address and port of Sia Daemon.
	RootDir  string // Root directory to store files on Sia.
	password string // Sia password for uploading content in authenticated manner.
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

// NewGatewayLayer returns Sia gateway layer, implements GatewayLayer interface to
// talk to Sia backend.
func (g *Sia) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	sia := &siaObjects{
		Address: g.host,
		// RootDir uses access key directly, provides partitioning for
		// concurrent users talking to same sia daemon.
		RootDir:  creds.AccessKey,
		password: os.Getenv("SIA_API_PASSWORD"),
	}

	// If Address not provided on command line or ENV, default to:
	if sia.Address == "" {
		sia.Address = "127.0.0.1:9980"
	}

	colorBlue := color.New(color.FgBlue).SprintfFunc()
	colorBold := color.New(color.Bold).SprintFunc()

	log.Println(colorBlue("\nSia Gateway Configuration:"))
	log.Println(colorBlue("  Sia Daemon API Address:") + colorBold(fmt.Sprintf(" %s\n", sia.Address)))
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
	// TODO (chrsch) Receive directory metadata for dir representing the bucket. For now return name only.
	return minio.BucketInfo{Name: bucket}, nil
}

// GetBucketPolicy - Sia doesn't implement any policies so return allow all
func (s *siaObjects) GetBucketPolicy(ctx context.Context, bucket string) (*policy.Policy, error) {
	return &policy.Policy{
		Version: policy.DefaultVersion,
		Statements: []policy.Statement{
			policy.NewStatement(
				policy.Allow,
				policy.NewPrincipal("*"),
				policy.NewActionSet(
					policy.GetBucketLocationAction,
					policy.ListBucketAction,
					policy.GetObjectAction,
				),
				policy.NewResourceSet(
					policy.NewResource(bucket, ""),
					policy.NewResource(bucket, "*"),
				),
				condition.NewFunctions(),
			),
		},
	}, nil
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
	// startOffset other than 0 not supported right now by Sia API
	if startOffset != 0 {
		return minio.InvalidRange{
			OffsetBegin: startOffset,
			OffsetEnd:   length,
		}
	}
	pr, pw := io.Pipe()
	siaObj := path.Join(s.RootDir, bucket, object)

	req, err := http.NewRequest("GET", "http://"+s.Address+"/renter/stream/"+siaObj, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Sia-Agent")
	req.Header.Set("Content-Type", "application/octet-stream")
	if s.password != "" {
		req.SetBasicAuth("", s.password)
	}

	c := http.Client{}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}

	bufferIn := bufio.NewReader(resp.Body)
	go func() {
		// close the writer, so the reader knows there's no more data
		defer pw.Close()

		// write data to the pipe writer
		bufferIn.WriteTo(pw)
	}()

	_, err = io.Copy(writer, pr)

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return MethodNotSupported{"/renter/stream/" + siaObj}
	}

	if non2xx(resp.StatusCode) {
		err := decodeError(resp)
		resp.Body.Close()
		return err
	}

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
	siaObj := path.Join(s.RootDir, bucket, object)

	req, err := http.NewRequest("POST", "http://"+s.Address+"/renter/uploadstream/"+siaObj, r.Reader)
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
