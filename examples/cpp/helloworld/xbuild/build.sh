
set -ex

# https://grpc.io/docs/languages/cpp/quickstart/

export MY_INSTALL_DIR=$HOME/.local

export PATH="$MY_INSTALL_DIR/bin:$PATH"

cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ..

# make -j 4
# make install

