![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/artashesbalabekyan/aistore)

## Reproduce

```console
$ git clone https://github.com/artashesbalabekyan/aistore.git
```

```console
$ cd aistore
```

```console
$ git checkout creds-via-props
```

Then run repo in devconainer and run the following commands in dev container

Step 1.
```console
$ make mod-all
```


Step 2.
```console
$ make kill clean cli && make deploy <<< '5\n5\n2\ny\ny\nn\nn\n0\n'
```

Wait for run

Try to set extra props

```console
$ ais bucket props set s3://<remote-bucket-name> extra.aws.access_key_id=<access_key_id>
```

Try to list remote bucket

```console
$ ais ls s3://<remote-bucket-name>
```

Export credentials via env variables

```console
$ export AWS_ACCESS_KEY_ID=<key_id>
$ export AWS_SECRET_ACCESS_KEY=<key>
$ export AWS_DEFAULT_REGION=<region>
```

Then run the step 2 command again to rerun the aistore
```console
$ make kill clean cli && make deploy <<< '5\n5\n2\ny\ny\nn\nn\n0\n'
```

Then try to list remote bucket or set the props

```console
$ ais ls s3://<remote-bucket-name>
```

```console
$ ais bucket props set s3://<remote-bucket-name> extra.aws.access_key_id=<access_key_id>
```