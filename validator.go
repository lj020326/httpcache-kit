package httpcache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
)

type Validator struct {
	Handler http.Handler
}

func (v *Validator) Validate(req *http.Request, res *Resource) bool {
	outreq := cloneRequest(req)
	resHeaders := res.Header()

	switch {
	case resHeaders.Get("Etag") != "":
		outreq.Header.Set("If-None-Match", resHeaders.Get("Etag"))
	case resHeaders.Get("Last-Modified") != "":
		outreq.Header.Set("If-Modified-Since", resHeaders.Get("Last-Modified"))
	default:
		// Nothing to validate WITH.
		//
		// The request would otherwise go upstream unconditionally and come
		// back with a full new body -- which this function ignores, comparing
		// only headers. headersEqual returns true when the new response
		// carries none of its four comparison headers, so the STORED body was
		// served and freshened as "validated" even though the resource had
		// changed underneath it. For an entry invalidated by a successful
		// POST/PUT/PATCH/DELETE that is exactly the case that must not happen.
		//
		// Report that validation is not possible; the caller fetches the
		// resource in full and replaces the entry.
		debugf("no validator (ETag or Last-Modified) on the cached response; cannot revalidate")
		return false
	}

	t := Clock()
	resp := httptest.NewRecorder()
	v.Handler.ServeHTTP(resp, outreq)
	resp.Flush()

	if age, err := correctedAge(resp.Header(), t, Clock()); err == nil {
		resp.Header().Set("Age", fmt.Sprintf("%.f", age.Seconds()))
	}

	if headersEqual(resHeaders, resp.Header()) {
		res.header = resp.Header()
		res.header.Set(ProxyDateHeader, Clock().Format(http.TimeFormat))
		return true
	}

	return false
}

var validationHeaders = []string{"ETag", "Content-MD5", "Last-Modified", "Content-Length"}

func headersEqual(h1, h2 http.Header) bool {
	for _, header := range validationHeaders {
		if value := h2.Get(header); value != "" {
			if h1.Get(header) != value {
				debugf("%s changed, %q != %q", header, value, h1.Get(header))
				return false
			}
		}
	}

	return true
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request) *http.Request {
	r2 := new(http.Request)
	*r2 = *r
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}
	return r2
}
