package utils

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	"github.com/gofrs/uuid"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func GenerateUUID() string {
	u2, err := uuid.NewV4()
	if err != nil {
		u2 = uuid.Must(uuid.NewV4())
	}
	return strings.ReplaceAll(u2.String(), "-", "")
}

func GetStringMd5(s string) string {
	md5 := md5.New()
	md5.Write([]byte(s))
	md5Str := hex.EncodeToString(md5.Sum(nil))
	return md5Str
}

func CheckSystem() (sysType uint32) {
	switch runtime.GOOS {
	case "windows":
		return 0x01
	case "linux":
		return 0x02
	default:
		return 0x03
	}
}

func GetSystemInfo() (string, string) {
	hostname, err := exec.Command("hostname").Output()
	if err != nil {
		hostname = []byte("Null")
	}
	username, err := exec.Command("whoami").Output()
	if err != nil {
		username = []byte("Null")
	}

	fHostname := strings.TrimRight(string(hostname), " \t\r\n")
	fUsername := strings.TrimRight(string(username), " \t\r\n")

	return fHostname, fUsername
}

func CheckIPPort(info string) (normalAddr string, reuseAddr string, err error) {
	var (
		readyIP   string
		readyPort int
	)

	spliltedInfo := strings.Split(info, ":")

	if len(spliltedInfo) == 1 {
		readyIP = "0.0.0.0"
		readyPort, err = strconv.Atoi(info)
	} else if len(spliltedInfo) == 2 {
		readyIP = spliltedInfo[0]
		readyPort, err = strconv.Atoi(spliltedInfo[1])
	} else {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	if err != nil || readyPort < 1 || readyPort > 65535 || readyIP == "" {
		err = errors.New("please input either port(1~65535) or ip:port(1-65535)")
		return
	}

	normalAddr = readyIP + ":" + strconv.Itoa(readyPort)
	reuseAddr = "0.0.0.0:" + strconv.Itoa(readyPort)

	return
}

func CheckRange(nodes []int) {
	for m := len(nodes) - 1; m > 0; m-- {
		var flag bool = false
		for n := 0; n < m; n++ {
			if nodes[n] > nodes[n+1] {
				temp := nodes[n]
				nodes[n] = nodes[n+1]
				nodes[n+1] = temp
				flag = true
			}
		}
		if !flag {
			break
		}
	}
}

func ConvertStr2GBK(str string) string {
	ret, err := simplifiedchinese.GBK.NewEncoder().String(str)
	if err != nil {
		ret = str
	}
	return ret
}

func ConvertGBK2Str(gbkStr string) string {
	ret, err := simplifiedchinese.GBK.NewDecoder().String(gbkStr)
	if err != nil {
		ret = gbkStr
	}
	return ret
}
