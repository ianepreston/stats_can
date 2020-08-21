# Only need to change these two variables
PKG_NAME=stats_can
USER=ian.e.preston

OS=$TRAVIS_OS_NAME-64
mkdir ~/conda-bld
conda config --set anaconda_upload yes
export CONDA_BLD_PATH=~/conda-bld
export VERSION=`date +%Y.%m.%d`

# Avoids packing large test files into the conda package
# Collect git information
GIT_COMMIT_HASH=`git rev-parse --verify HEAD`
GIT_REMOTE=`git config --get remote.origin.url`
# Prevent packaging in conda release
rm -r tests/test_files
# Script for manual downloading of test_files into tests directory
echo -e "# Script only provided with package to allow to download necessary data for test suite" >> tests/download_test_files.sh
echo -e "# Run script before running test suite" >> tests/download_test_files.sh
echo -e "git clone $GIT_REMOTE\ncd stats_can\ngit checkout $GIT_COMMIT_HASH" >> tests/download_test_files.sh
echo -e "mv tests/test_files ..\ncd ..\nrm -rf stats_can" >> tests/download_test_files.sh

conda build . --token $CONDA_UPLOAD_TOKEN --user $USER
