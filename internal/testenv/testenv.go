// Package testenv starts one real kube-apiserver + etcd (controller-runtime's
// envtest) per test binary and hands out clients to it. Integration tests use
// it so they exercise the real API machinery (graceful deletion, UID
// preconditions, watches, RBAC, Leases), which no fake clientset reproduces.
//
// There is no kubelet and no controller-manager in this environment, which is
// exactly the situation shepherd exists for: a pod that is deleted gracefully
// and scheduled to a node stays Terminating forever, because nothing ever
// confirms its termination.
//
// If the apiserver/etcd binaries are not available (KUBEBUILDER_ASSETS unset)
// the tests are skipped, not failed; `make integration` fetches them.
package testenv

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	once    sync.Once
	env     *envtest.Environment
	cfg     *rest.Config
	startEr error
)

// Config returns an admin (system:masters) rest config, starting the control
// plane on first use. It skips the calling test if no binaries are available.
func Config(t testing.TB) *rest.Config {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make integration` (fetches kube-apiserver and etcd)")
	}
	once.Do(func() {
		env = &envtest.Environment{}
		cfg, startEr = env.Start()
	})
	if startEr != nil {
		t.Fatalf("starting envtest control plane: %v", startEr)
	}
	return rest.CopyConfig(cfg)
}

// Client returns a client with admin rights.
func Client(t testing.TB) kubernetes.Interface {
	t.Helper()
	c, err := kubernetes.NewForConfig(Config(t))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// UserConfig returns a rest config authenticated as a certificate user with
// the given name and NO permissions until RBAC grants some. This is how the
// RBAC test runs shepherd with exactly the rules it ships.
func UserConfig(t testing.TB, name string) *rest.Config {
	t.Helper()
	admin := Config(t)
	u, err := env.AddUser(envtest.User{Name: name}, admin)
	if err != nil {
		t.Fatalf("adding user %q: %v", name, err)
	}
	return u.Config()
}

// Stop shuts the control plane down. Call from TestMain after m.Run.
func Stop() {
	if env != nil {
		if err := env.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "stopping envtest:", err)
		}
	}
}
