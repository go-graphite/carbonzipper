package tls

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/fastly/go-utils/executable"
	"github.com/fastly/go-utils/vlog"
)

type AuthStatus int

const (
	AuthSuccess AuthStatus = iota
	AuthFailure
)

var (
	_certPath,
	_adminuser,
	_adminpass,
	_authrealm string
	_insecure bool
	// AuthRequest is called on handler authorizations.
	// An example use is setting this function to log
	// valid or invalid authorizations.
	AuthRequest func(r *http.Request, status AuthStatus)
)

// Init sets the CertPath to search for TLS certs and keys. If CertPath is empty, $BIN/../certs
// and $PWD/../../../../certs are searched. Insecure is a flag to ignore cert verification errors.
func Init(certPath string, insecure bool) {
	_certPath = certPath
	_insecure = insecure
}

var packagedCertDir string

// LocatePackagedPEMDir locates the path of the packaged PEM store which is the
// directory named "certs". functions that take a (name string) parameter look
// for files named ${name}-key.pem and/or ${name}-cert.pem in this directory.
func LocatePackagedPEMDir() (dir string, err error) {
	if packagedCertDir != "" {
		dir = packagedCertDir
		return
	} else if _certPath != "" {
		packagedCertDir = _certPath
		dir = packagedCertDir
		return
	}

	var binDir string
	binDir, err = executable.Dir()
	if err != nil {
		return
	}

	cwd, _ := os.Getwd()

	searchList := []string{
		binDir + "../certs", // deb: certs is one up from bin
		cwd + "/testcerts",  // tests: certs is 4 up from _test files
	}

	// src-containing git checkout: $GOPATH/certs
	if goPath := os.Getenv("GOPATH"); goPath != "" {
		for _, gp := range filepath.SplitList(goPath) {
			searchList = append(searchList, gp+"/certs")
		}
	}

	// non-src git checkout: search for a certs/ starting deepest-first at
	// $GOPATH/src/foo/bar/, where foo/bar is the package calling into this
	// one. This works if foo/bar has subpackages which call this function,
	// even indirectly by way of the other functions in this package.
	if callerDir, ok := callerPkgDir(); ok {
		if certsDir, ok := searchUpwards(callerDir, "certs"); ok {
			searchList = append(searchList, certsDir)
		}
	}

	for _, l := range searchList {
		d := filepath.Clean(l)
		var info os.FileInfo
		if info, err = os.Stat(d); err == nil && info.IsDir() {
			packagedCertDir = d
			dir = packagedCertDir
			return
		}
	}
	err = fmt.Errorf("couldn't locate packaged PEMs in any of %v", searchList)
	return
}

// callerPkgDir returns a pathname containing the build-time location of the
// deepest calling function that is not within this package. If that cannot be
// found, ok will be false and dir will be empty.
func callerPkgDir() (dir string, ok bool) {
	_, thisFile, _, cok := runtime.Caller(0) // /home/me/workspace/src/fastly/go-utils/tls/tls.go
	if !cok {
		return
	}

	thisDir := filepath.Dir(thisFile)                                 // /home/me/workspace/src/fastly/go-utils/tls
	sep := fmt.Sprintf("%csrc%c", os.PathSeparator, os.PathSeparator) // /src/
	p := strings.SplitN(thisDir, sep, 2)
	if len(p) < 2 {
		return
	}
	thisPkg := p[1] // fastly/go-utils/tls/

	for i := 1; ; i++ {
		if i > 10 {
			// typical stack is tls.ConfigureServer -> tls.GenerateConfig ->
			// tls.LoadPackagedKeypair -> tls.LocatePackagedPEMFile ->
			// tls.LocatePackagedPEMDir -> tls.searchUpwards
			panic("excessive recursion inside tls package")
		}

		_, file, _, cok := runtime.Caller(i) // /home/me/workspace/src/foo/bar/subpkg/thing.go
		if !cok {
			return
		}

		if !strings.Contains(file, thisPkg) {
			return filepath.Dir(file), true // /home/me/workspace/src/foo/bar/subpkg
		}
	}
}

// searchUpwards finds the deepest directory that is `start` or one of its
// parents which contains a directory named `dir`. The returned path is to the
// search target, not its parent. If none could be found, ok will be false and
// path will be empty.
func searchUpwards(start, dir string) (path string, ok bool) {
	cur := start
	for {
		test := filepath.Join(cur, dir)
		fi, err := os.Stat(test)
		if err == nil && fi != nil && fi.IsDir() {
			return test, true
		}
		next := filepath.Dir(cur)
		if next == cur {
			return
		}
		cur = next
	}
}

// LocatePackagedPEMFile loads a single PEM file (with -cert or -key suffix) from the package store
func LocatePackagedPEMFile(name string) (file string, err error) {
	if strings.IndexRune(name, '/') < 0 {
		var dir string
		if dir, err = LocatePackagedPEMDir(); err != nil {
			return
		}
		f := filepath.Join(dir, name+".pem")
		if _, err = os.Stat(f); err != nil {
			return
		}
		file = f
	} else {
		// explicit path
		if _, err = os.Stat(name); err != nil {
			return "", fmt.Errorf("Cert(s) at path %q couldn't be loaded: %s", name, err)
		}
		file = name
	}
	return
}

// LoadPackagedKeypair loads a cert/key pair from the package store
// It looks for the ${name}-[cert,key].pem files from either the PEM dir
// if just a filename is given or from the fullpath if a path is given.
func LoadPackagedKeypair(name string) (cert tls.Certificate, certFile, keyFile string, err error) {
	certFile, err = LocatePackagedPEMFile(name + "-cert")
	if err != nil {
		return
	}
	keyFile, err = LocatePackagedPEMFile(name + "-key")
	if err != nil {
		return
	}
	cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	return
}

// GenerateConfig returns a *tls.Config for either a client if true or server if client
// is false, the given key pair ${name}-[cert,key].pem files and accepting the caCertNames
// if given.
func GenerateConfig(client bool, keyPairName string, caCertNames []string) (config *tls.Config, err error) {
	config = &tls.Config{
		InsecureSkipVerify: _insecure,
		MinVersion:         tls.VersionTLS12,
	}

	label := "server"
	if client {
		label = "client"
	}

	var keyPair tls.Certificate
	var cFile, kFile string
	if keyPairName != "" {
		keyPair, cFile, kFile, err = LoadPackagedKeypair(keyPairName)
		if err != nil {
			return
		}

		vlog.VLogf("Loaded packaged %s keypair from %s and %s", label, cFile, kFile)
		config.Certificates = []tls.Certificate{keyPair}
	}

	if len(caCertNames) > 0 {
		caPool := x509.NewCertPool()
		if client {
			config.RootCAs = caPool
		} else {
			config.ClientCAs = caPool
			if _insecure {
				config.ClientAuth = tls.RequestClientCert
			} else {
				config.ClientAuth = tls.RequireAndVerifyClientCert
			}
		}

		for _, name := range caCertNames {
			n := name
			if strings.Index(name, "/") < 0 {
				n = name + "-cert"
			}
			var file string
			if file, err = LocatePackagedPEMFile(n); err != nil {
				return nil, fmt.Errorf("Failed to find cert named %q: %s", name, err)
			}
			var b []byte
			if b, err = ioutil.ReadFile(file); err != nil {
				return nil, fmt.Errorf("Can't read cert from %s: %s", file, err)
			}

			vlog.VLogf("Allowing %s CA cert from %s", label, file)
			if ok := caPool.AppendCertsFromPEM(b); !ok {
				return nil, fmt.Errorf("No cert could be found in %s", file)
			}
		}
	}
	return
}

// ConfigureServer returns a TLS server configuration that presents serverKeyPairName to
// clients. if clientCertNames is non-empty the server will request a client
// certificate and require that it be provided and signed by one of the named
// certs.
func ConfigureServer(serverKeyPairName string, clientCertNames ...string) (config *tls.Config, err error) {
	config, err = GenerateConfig(false, serverKeyPairName, clientCertNames)
	if config != nil {
		config.CipherSuites = PreferredCipherSuites()
	}
	return
}

// ConfigureClient returns a TLS client configuration that presents clientKeyPair
// to the remote server. if serverCertNames is non-empty, server certificates must
// be signed by one of the named certs; otherwise the default system CA list will be
// used.
func ConfigureClient(clientKeyPairName string, serverCertNames ...string) (config *tls.Config, err error) {
	return GenerateConfig(true, clientKeyPairName, serverCertNames)
}

// PreferredCipherSuites returns the set of default cipher suites, minus RC4
// and 3DES suites, with ECDSA preferred over RSA.
func PreferredCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	}
}

// SetWrapCreds stores the adminuser, adminpass, and authrealm. These parameters
// will be used as the credentials and realm in calls to WrapHandleForAuth
// and WrapHandlerFuncForAuth.
func SetWrapCreds(adminuser, adminpass, authrealm string) {
	_adminuser = adminuser
	_adminpass = adminpass
	_authrealm = authrealm
}

// WrapHandlerForAuth calls WrapHandlerForAuthCreds with the currently stored
// adminuser, adminpass, and authrealm. SetWrapCreds should be called before this function
// or else the HAndler will not be wrapped with basic authentication.
func WrapHandlerForAuth(h http.Handler) http.Handler {
	return WrapHandlerForAuthCreds(h, _adminuser, _adminpass, _authrealm)
}

// WrapHandlerFuncForAuth calls WrapHandlerFuncForAuthCreds with the currently stored
// adminuser, adminpass, and authrealm. SetWrapCreds should be called before this function
// or else the HandlerFunc will not be wrapped with basic authentication.
func WrapHandlerFuncForAuth(h http.HandlerFunc) http.HandlerFunc {
	return WrapHandlerFuncForAuthCreds(h, _adminuser, _adminpass, _authrealm)
}

// WrapHandlerForAuthCreds returns the Handler wrapped with basic authentication
// requiring credentials adminuser and adminpass. The authrealm will be used
// for the WWW-Authenticate header's basic realm.
func WrapHandlerForAuthCreds(h http.Handler, adminuser, adminpass, authrealm string) http.Handler {
	if adminuser == "" && adminpass == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAuthenticated(r, adminuser, adminpass) {
			if AuthRequest != nil {
				AuthRequest(r, AuthSuccess)
			}
			h.ServeHTTP(w, r)
		} else {
			if AuthRequest != nil {
				AuthRequest(r, AuthFailure)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="`+authrealm+`"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized\n"))
		}
	})
}

// WrapHandlerFuncForAuth returns the HandlerFunc wrapped with basic authentication
// requiring credentials adminuser and adminpass. The authrealm will be used
// for the WWW-Authenticate header's basic realm.
func WrapHandlerFuncForAuthCreds(h http.HandlerFunc, adminuser, adminpass, authrealm string) http.HandlerFunc {
	if adminuser == "" && adminpass == "" {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAuthenticated(r, adminuser, adminpass) {
			if AuthRequest != nil {
				AuthRequest(r, AuthSuccess)
			}
			h(w, r)
		} else {
			if AuthRequest != nil {
				AuthRequest(r, AuthFailure)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="`+authrealm+`"`)
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized\n"))
		}
	})
}

func isAuthenticated(r *http.Request, adminuser, adminpass string) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	pieces := strings.Split(auth, " ")
	if len(pieces) != 2 || pieces[0] != "Basic" {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(pieces[1])
	if err != nil {
		return false
	}
	userpass := strings.Split(string(decoded), ":")
	if len(userpass) != 2 || userpass[0] != adminuser || userpass[1] != adminpass {
		return false
	}
	return true
}
