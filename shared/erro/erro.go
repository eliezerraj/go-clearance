//---------------------------------------
// Component is charge of defined message errors
//---------------------------------------
package erro

import (
	"errors"
)

var (
	ErrNotFound 		= errors.New("item not found")
	ErrBadRequest 		= errors.New("bad request! check parameters")
	ErrUpdate			= errors.New("update unsuccessful")
	ErrInsert 			= errors.New("insert data error")
	ErrUnmarshal 		= errors.New("unmarshal json error")
	ErrUnauthorized 	= errors.New("not authorized")
	ErrServer		 	= errors.New("server identified error")
	ErrHTTPForbiden		= errors.New("forbiden request")
	ErrTimeout			= errors.New("timeout: context deadline exceeded")
	ErrHealthCheck		= errors.New("health check services required failed")
)
