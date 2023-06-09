// TODO: generic implementation of KES, for retrieving the secret values and using those values in a backend
// More information: https://pkg.go.dev/github.com/minio/kes-go#Client.ReadSecret
// More information: https://github.com/minio/kes
// More information: https://github.com/minio/kes/wiki/Filesystem-Keystore

package cluster

type KesClient struct {
	client Client
}

var c KesClient

func GetAwsAccessKey(sprefix string) (svalue string, err error) {
	return GetSecretValue(sprefix + "_ACCESS_KEY_ID")
}

func GetAwsSecretAccess(sprefix string) (svalue string, err error) {
	return GetSecretValue(sprefix + "_SECRET_ACCESS_KEY")
}

func GetAwsRegion(sprefix string) (svalue string, err error) {
	return GetSecretValue(sprefix + "_REGION")
}

func GetAwsEndpoint(sprefix string) (svalue string, err error) {
	return GetSecretValue(sprefix + "_ENDPOINT")
}

func GetSecretValue(sname string) (svalue string, err error) {
	c.init()
	panic("GetSecretValue function is not implemented")
}

func (c KesClient) init() {
	if c == (KesClient{}) {
		// init kes
		// Using environment variables:
		// AIS_KES_ENDPOINTS
		// AIS_KES_CERT_DIR
	}
}
