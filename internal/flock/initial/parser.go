package initial

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"

	"codeberg.org/agnoie/shepherd/pkg/share/handshake"
)

const (
	NORMAL_ACTIVE = iota
	NORMAL_RECONNECT_ACTIVE
	NORMAL_PASSIVE
	SOCKS5_PROXY_ACTIVE
	HTTP_PROXY_ACTIVE
	SOCKS5_PROXY_RECONNECT_ACTIVE
	HTTP_PROXY_RECONNECT_ACTIVE
	SO_REUSE_PASSIVE
	IPTABLES_REUSE_PASSIVE
)

const minSecretLength = 8

type Options struct {
	Mode          int
	Secret        string
	baseSecret    string
	Listen        string
	Reconnect     uint64
	Connect       string
	ReuseHost     string
	ReusePort     string
	Socks5Proxy   string
	Socks5ProxyU  string
	Socks5ProxyP  string
	HttpProxy     string
	Upstream      string
	Downstream    string
	Charset       string
	Domain        string
	TlsEnable     bool
	HTTPHost      string
	HTTPPath      string
	HTTPUserAgent string
	PreAuthToken  string
	MFAPin        string
	RepairBind    string
	RepairPort    int
	// 短连接/DTN：期望唤醒周期（秒）；0 表示长连接。
	SleepSeconds int
	WorkSeconds  int
	SleepJitter  float64
}

var args *Options

func init() {
	args = new(Options)

	flag.StringVar(&args.Secret, "s", "", "")
	flag.StringVar(&args.Listen, "l", "", "")
	flag.Uint64Var(&args.Reconnect, "reconnect", 0, "")
	flag.StringVar(&args.Connect, "c", "", "")
	flag.StringVar(&args.ReuseHost, "rehost", "", "")
	flag.StringVar(&args.ReusePort, "report", "", "")
	flag.StringVar(&args.Socks5Proxy, "socks5-proxy", "", "")
	flag.StringVar(&args.Socks5ProxyU, "socks5-proxyu", "", "")
	flag.StringVar(&args.Socks5ProxyP, "socks5-proxyp", "", "")
	flag.StringVar(&args.HttpProxy, "http-proxy", "", "")
	flag.StringVar(&args.Upstream, "up", "raw", "")
	flag.StringVar(&args.Downstream, "down", "raw", "")
	flag.StringVar(&args.Charset, "cs", "utf-8", "")
	flag.StringVar(&args.Domain, "domain", "", "")
	flag.BoolVar(&args.TlsEnable, "tls-enable", false, "")
	flag.StringVar(&args.HTTPHost, "http-host", "", "")
	flag.StringVar(&args.HTTPPath, "http-path", "", "")
	flag.StringVar(&args.HTTPUserAgent, "http-user-agent", "", "")
	flag.StringVar(&args.MFAPin, "mfa-pin", "", "")
	flag.StringVar(&args.RepairBind, "repair-bind", "", "")
	flag.IntVar(&args.RepairPort, "repair-port", 0, "")
	flag.IntVar(&args.SleepSeconds, "sleep", 0, "")
	flag.IntVar(&args.WorkSeconds, "work", 10, "")
	flag.Float64Var(&args.SleepJitter, "sleep-jitter", 0.2, "")

	flag.Usage = func() {}
}

// ParseOptions 解析 CLI 参数并完成合法性校验。
func ParseOptions() (*Options, error) {
	flag.Parse()

	switch {
	case args.Listen != "" && args.Connect == "" && args.Reconnect == 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy == "" && args.Socks5ProxyU == "" && args.Socks5ProxyP == "" && args.HttpProxy == "":
		args.Mode = NORMAL_PASSIVE
		logger.Infof("starting agent node passively; listening on port %s", args.Listen)
	case args.Listen == "" && args.Connect != "" && args.Reconnect == 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy == "" && args.Socks5ProxyU == "" && args.Socks5ProxyP == "" && args.HttpProxy == "":
		args.Mode = NORMAL_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s", args.Connect)
	case args.Listen == "" && args.Connect != "" && args.Reconnect != 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy == "" && args.Socks5ProxyU == "" && args.Socks5ProxyP == "" && args.HttpProxy == "":
		args.Mode = NORMAL_RECONNECT_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s and reconnecting every %d seconds", args.Connect, args.Reconnect)
	case args.Listen == "" && args.Connect == "" && args.Reconnect == 0 && args.ReuseHost != "" && args.ReusePort != "" && args.Socks5Proxy == "" && args.Socks5ProxyU == "" && args.Socks5ProxyP == "" && args.HttpProxy == "":
		args.Mode = SO_REUSE_PASSIVE
		logger.Infof("starting agent node passively; reusing host %s port %s (SO_REUSEPORT, SO_REUSEADDR)", args.ReuseHost, args.ReusePort)
	case args.Listen != "" && args.Connect == "" && args.Reconnect == 0 && args.ReuseHost == "" && args.ReusePort != "" && args.Socks5Proxy == "" && args.Socks5ProxyU == "" && args.Socks5ProxyP == "" && args.HttpProxy == "":
		args.Mode = IPTABLES_REUSE_PASSIVE
		logger.Infof("starting agent node passively; reusing port %s (iptables)", args.ReusePort)
	case args.Listen == "" && args.Connect != "" && args.Reconnect == 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy != "" && args.HttpProxy == "":
		args.Mode = SOCKS5_PROXY_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s via socks5 proxy %s", args.Connect, args.Socks5Proxy)
	case args.Listen == "" && args.Connect != "" && args.Reconnect != 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy != "" && args.HttpProxy == "":
		args.Mode = SOCKS5_PROXY_RECONNECT_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s via socks5 proxy %s and reconnecting every %d seconds", args.Connect, args.Socks5Proxy, args.Reconnect)
	case args.Listen == "" && args.Connect != "" && args.Reconnect == 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy == "" && args.HttpProxy != "":
		args.Mode = HTTP_PROXY_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s via http proxy %s", args.Connect, args.HttpProxy)
	case args.Listen == "" && args.Connect != "" && args.Reconnect != 0 && args.ReuseHost == "" && args.ReusePort == "" && args.Socks5Proxy == "" && args.HttpProxy != "":
		args.Mode = HTTP_PROXY_RECONNECT_ACTIVE
		logger.Infof("starting agent node actively; connecting to %s via http proxy %s and reconnecting every %d seconds", args.Connect, args.HttpProxy, args.Reconnect)
	default:
		return nil, fmt.Errorf("invalid argument combination")
	}

	if args.Secret == "" {
		return nil, fmt.Errorf("secret must not be empty")
	}
	if len(args.Secret) < minSecretLength {
		return nil, fmt.Errorf("secret must be at least %d characters", minSecretLength)
	}
	if err := handshake.ValidateSecretComplexity(args.Secret); err != nil {
		return nil, err
	}
	if err := handshake.ValidateMFAPin(args.MFAPin); err != nil {
		return nil, err
	}

	if args.Charset != "utf-8" && args.Charset != "gbk" {
		return nil, fmt.Errorf("charset must be 'utf-8' or 'gbk'")
	}

	if args.RepairBind == "" {
		args.RepairBind = "0.0.0.0"
	}
	if args.RepairPort < 0 || args.RepairPort > 65535 {
		return nil, fmt.Errorf("repair-port must be in range 0-65535")
	}

	if args.Domain == "" && args.Connect != "" {
		addrSlice := strings.SplitN(args.Connect, ":", 2)
		args.Domain = addrSlice[0]
	}

	if err := checkOptions(args); err != nil {
		return nil, err
	}

	// 为下游握手保留原始密钥；当前会话密钥可能会在首条链路建立后被覆盖。
	args.baseSecret = args.Secret

	return args, nil
}

// BaseSecret 返回通过 CLI 传入的原始密钥。
// 之所以单独保留它，是因为握手后 Options.Secret 可能会被派生出的会话密钥覆盖。
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
	if strings.TrimSpace(option.Connect) != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.Connect); err != nil {
			return err
		}
	}

	if strings.TrimSpace(option.Socks5Proxy) != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.Socks5Proxy); err != nil {
			return err
		}
	}

	if strings.TrimSpace(option.HttpProxy) != "" {
		if _, err := net.ResolveTCPAddr("tcp", option.HttpProxy); err != nil {
			return err
		}
	}

	if strings.TrimSpace(option.ReuseHost) != "" {
		if addr := net.ParseIP(option.ReuseHost); addr == nil {
			return errors.New("ReuseHost is not a valid IP addr")
		}
	}

	if strings.TrimSpace(option.RepairBind) != "" {
		if addr := net.ParseIP(option.RepairBind); addr == nil {
			return fmt.Errorf("repair-bind is not a valid IP addr")
		}
	}

	return nil
}
