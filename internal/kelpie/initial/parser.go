package initial

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
)

const (
	NORMAL_ACTIVE = iota
	NORMAL_PASSIVE
	SOCKS5_PROXY_ACTIVE
	HTTP_PROXY_ACTIVE
)

const (
	UIBackendGRPC = "grpc"
)

const minSecretLength = 8

type Options struct {
	Mode          uint8
	Secret        string
	baseSecret    string
	Listen        string
	Connect       string
	Socks5Proxy   string
	Socks5ProxyU  string
	Socks5ProxyP  string
	HttpProxy     string
	Downstream    string
	Domain        string
	TlsEnable     bool
	Heartbeat     bool
	HTTPHost      string
	HTTPPath      string
	HTTPUserAgent string
	PreAuthToken  string
	MFAPin        string
	// SleepSeconds: 期望的短连接唤醒周期（秒）。
	// 0 表示长连接（常唤醒），用于离线判定的“立即/极短”策略。
	SleepSeconds   int
	UIBackend      string
	UIListen       string
	UIEnableTLS    bool
	UITLSCert      string
	UITLSKey       string
	UITLSClientCA  string
	UIAuthToken    string
	UIRateLimitRPS float64

	// dataplane: 专用数据通路监听/限流/TTL。
	DataListen       string
	DataEnableTLS    bool
	DataTLSCert      string
	DataTLSKey       string
	DataTLSClientCA  string
	DataMaxRateBps   float64
	DataMaxSize      int64
	DataTokenTTL     time.Duration
	DataTokenRetries int

	// TLS CA 证书导出：自动导出 CA 证书供 Stockman 客户端使用。
	ExportCAPath string
}

func newFlagSet(opts *Options) *flag.FlagSet {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	// 核心参数：密钥 + gRPC UI 监听 + Teamserver 令牌 + 可选 UI TLS。
	fs.StringVar(&opts.Secret, "s", "", "Communication secret (shared with Flock)")
	fs.StringVar(&opts.UIListen, "ui-grpc-listen", "127.0.0.1:50061", "Listen address for the gRPC UI server")
	fs.BoolVar(&opts.UIEnableTLS, "ui-grpc-tls", false, "Enable TLS for the gRPC UI server (requires cert/key)")
	fs.StringVar(&opts.UITLSCert, "ui-grpc-cert", "", "Path to TLS certificate for the gRPC UI server")
	fs.StringVar(&opts.UITLSKey, "ui-grpc-key", "", "Path to TLS private key for the gRPC UI server")
	fs.StringVar(&opts.UITLSClientCA, "ui-grpc-client-ca", "", "Optional client CA to enforce mutual TLS")
	fs.StringVar(&opts.UIAuthToken, "ui-grpc-token", "", "Teamserver token required by gRPC UI clients")
	fs.Float64Var(&opts.UIRateLimitRPS, "ui-grpc-rate", 20.0, "Max gRPC UI requests per second (0 = unlimited)")

	// Dataplane（数据通路）参数：独立监听、TLS、速率/大小/TTL。
	fs.StringVar(&opts.DataListen, "dataplane-listen", "127.0.0.1:60080", "Listen address for dataplane service (data channel)")
	fs.BoolVar(&opts.DataEnableTLS, "dataplane-tls", false, "Enable TLS for dataplane service")
	fs.StringVar(&opts.DataTLSCert, "dataplane-cert", "", "Path to TLS certificate for dataplane service")
	fs.StringVar(&opts.DataTLSKey, "dataplane-key", "", "Path to TLS private key for dataplane service")
	fs.StringVar(&opts.DataTLSClientCA, "dataplane-client-ca", "", "Optional client CA for mutual TLS on dataplane")
	fs.Float64Var(&opts.DataMaxRateBps, "dataplane-max-rate", 0, "Max bytes/sec per transfer on dataplane (0 = unlimited)")
	fs.Int64Var(&opts.DataMaxSize, "dataplane-max-size", 0, "Max bytes per transfer on dataplane (0 = unlimited)")
	fs.DurationVar(&opts.DataTokenTTL, "dataplane-token-ttl", 10*time.Minute, "TTL for dataplane one-time tokens")
	fs.IntVar(&opts.DataTokenRetries, "dataplane-token-retries", 2, "Retry count for dataplane tokens on transient errors")

	// TLS CA 证书导出配置
	fs.StringVar(&opts.ExportCAPath, "export-ca", "", "Path to export the CA certificate PEM for Stockman clients (e.g. ./kelpie-ca.pem)")

	fs.Usage = newUsage(fs)
	return fs
}

func newUsage(fs *flag.FlagSet) func() {
	return func() {
		var output io.Writer = os.Stderr
		if fs != nil && fs.Output() != nil {
			output = fs.Output()
		}
		fmt.Fprintf(output, `
Usages:
	>> ./kelpie -s <secret> [--ui-grpc-listen 127.0.0.1:50061] [--ui-grpc-token <token>]
	   [--ui-grpc-tls --ui-grpc-cert <cert> --ui-grpc-key <key> --ui-grpc-client-ca <ca>]
	   [--ui-grpc-rate <rps>]
`)
		if fs != nil {
			fs.PrintDefaults()
		}
	}
}

// ParseOptions 解析 CLI 参数并完成合法性校验。
func ParseOptions() (*Options, error) {
	return parseOptions(os.Args[1:])
}

func parseOptions(arguments []string) (*Options, error) {
	options := new(Options)
	flagSet := newFlagSet(options)
	if err := flagSet.Parse(arguments); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			flagSet.Usage()
		}
		return nil, err
	}

	// Kelpie 始终作为 teamserver 运行；Admin <-> Flock 的入口由 Controller Listener 决定。
	options.Mode = NORMAL_PASSIVE

	if options.Secret == "" {
		return nil, fmt.Errorf("secret must not be empty")
	}
	if len(options.Secret) < minSecretLength {
		return nil, fmt.Errorf("secret must be at least %d characters", minSecretLength)
	}
	if err := handshake.ValidateSecretComplexity(options.Secret); err != nil {
		return nil, err
	}
	if err := handshake.ValidateMFAPin(options.MFAPin); err != nil {
		return nil, err
	}

	if options.Domain == "" && options.Connect != "" {
		addrSlice := strings.SplitN(options.Connect, ":", 2)
		options.Domain = addrSlice[0]
	}

	if err := checkOptions(options); err != nil {
		return nil, err
	}

	options.UIBackend = strings.ToLower(strings.TrimSpace(options.UIBackend))
	if options.UIBackend == "" {
		options.UIBackend = UIBackendGRPC
	}
	if options.UIBackend != UIBackendGRPC {
		return nil, fmt.Errorf("unknown ui-backend: %s", options.UIBackend)
	}
	if options.UIBackend == UIBackendGRPC {
		if options.UIListen == "" {
			options.UIListen = "127.0.0.1:50061"
		}
		if _, err := net.ResolveTCPAddr("tcp", options.UIListen); err != nil {
			return nil, fmt.Errorf("invalid ui-grpc-listen: %w", err)
		}
		if strings.TrimSpace(options.UIAuthToken) == "" {
			return nil, fmt.Errorf("ui-grpc-token is required")
		}
		if options.UIEnableTLS {
			if strings.TrimSpace(options.UITLSCert) == "" || strings.TrimSpace(options.UITLSKey) == "" {
				return nil, fmt.Errorf("ui-grpc-tls requires both ui-grpc-cert and ui-grpc-key")
			}
			if err := ensureFileExists(options.UITLSCert); err != nil {
				return nil, fmt.Errorf("ui-grpc-cert not accessible: %w", err)
			}
			if err := ensureFileExists(options.UITLSKey); err != nil {
				return nil, fmt.Errorf("ui-grpc-key not accessible: %w", err)
			}
		}
		if options.UITLSClientCA != "" {
			if err := ensureFileExists(options.UITLSClientCA); err != nil {
				return nil, fmt.Errorf("ui-grpc-client-ca not accessible: %w", err)
			}
		}
		if options.UIRateLimitRPS < 0 {
			return nil, fmt.Errorf("ui-grpc-rate must be >= 0")
		}
	}

	// Dataplane 校验
	if strings.TrimSpace(options.DataListen) == "" {
		options.DataListen = "127.0.0.1:60080"
	}
	if _, err := net.ResolveTCPAddr("tcp", options.DataListen); err != nil {
		return nil, fmt.Errorf("invalid dataplane-listen: %w", err)
	}
	if options.DataEnableTLS {
		if strings.TrimSpace(options.DataTLSCert) == "" || strings.TrimSpace(options.DataTLSKey) == "" {
			return nil, fmt.Errorf("dataplane-tls requires dataplane-cert and dataplane-key")
		}
		if err := ensureFileExists(options.DataTLSCert); err != nil {
			return nil, fmt.Errorf("dataplane-cert not accessible: %w", err)
		}
		if err := ensureFileExists(options.DataTLSKey); err != nil {
			return nil, fmt.Errorf("dataplane-key not accessible: %w", err)
		}
		if options.DataTLSClientCA != "" {
			if err := ensureFileExists(options.DataTLSClientCA); err != nil {
				return nil, fmt.Errorf("dataplane-client-ca not accessible: %w", err)
			}
		}
	}
	if options.DataMaxRateBps < 0 {
		return nil, fmt.Errorf("dataplane-max-rate must be >= 0")
	}
	if options.DataMaxSize < 0 {
		return nil, fmt.Errorf("dataplane-max-size must be >= 0")
	}
	if options.DataTokenRetries < 0 {
		return nil, fmt.Errorf("dataplane-token-retries must be >= 0")
	}
	if options.DataTokenTTL <= 0 {
		return nil, fmt.Errorf("dataplane-token-ttl must be > 0")
	}

	// 保存一份原始密钥用于后续握手重用，避免在会话过程中被覆写。
	options.baseSecret = options.Secret

	return options, nil
}

// BaseSecret 返回初始配置时的原始密钥。
// 如果调用方尚未设置，默认回退到当前 Secret，并缓存下来。
func (opt *Options) BaseSecret() string {
	if opt == nil {
		return ""
	}
	if opt.baseSecret == "" {
		opt.baseSecret = opt.Secret
	}
	return opt.baseSecret
}

func checkOptions(option *Options) error {
	if option == nil {
		return fmt.Errorf("nil options")
	}
	if option.Connect != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.Connect); err != nil {
			return err
		}
	}

	if option.Socks5Proxy != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.Socks5Proxy); err != nil {
			return err
		}
	}

	if option.HttpProxy != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.HttpProxy); err != nil {
			return err
		}
	}
	return nil
}

func ensureFileExists(path string) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	return nil
}
