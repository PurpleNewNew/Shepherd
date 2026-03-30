package auth

import "context"

const (
	RoleAdmin = "admin"
)

type Claims struct {
	Username string
	Role     string
	// Tenant 用于多租场景下的审计/隔离标签。
	// 在当前 teamserver 模式中由入口拦截器设置，不参与权限判断。
	Tenant    string
	SessionID string
}

type claimsContextKey struct{}

func ContextWithClaims(ctx context.Context, claims Claims) context.Context {
	return context.WithValue(ctx, claimsContextKey{}, claims)
}

func ClaimsFromContext(ctx context.Context) (Claims, bool) {
	if ctx == nil {
		return Claims{}, false
	}
	val, ok := ctx.Value(claimsContextKey{}).(Claims)
	return val, ok
}
