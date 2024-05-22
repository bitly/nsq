module github.com/nsqio/nsq

go 1.17

require (
	github.com/BurntSushi/toml v1.3.2
	github.com/bitly/go-hostpool v0.1.0
	github.com/bitly/timer_metrics v1.0.0
	github.com/blang/semver v3.5.1+incompatible
	github.com/bmizerany/perks v0.0.0-20141205001514-d9a9656a3a4b
	github.com/golang/snappy v0.0.4
	github.com/judwhite/go-svc v1.2.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/mreiferson/go-options v1.0.0
	github.com/nsqio/go-diskqueue v1.1.0
	github.com/nsqio/go-nsq v1.1.0
)

require (
	github.com/stretchr/testify v1.9.0 // indirect
	golang.org/x/sys v0.10.0 // indirect
)

replace github.com/judwhite/go-svc => github.com/mreiferson/go-svc v1.2.2-0.20210815184239-7a96e00010f6
