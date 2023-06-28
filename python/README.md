# Troubleshooting

## - `confluent_kafka==1.7.0` dependency cannot install

When trying to install dependencies from the requirements file if you receive:

`clang: error: Invalid version number in 'MACOSX_DEPLOYMENT_TARGET=13`

The issue may be your version of Python. The known working version is Python3.9, 
please ensure you are using this version and attempt to install again.