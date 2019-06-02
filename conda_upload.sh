# Only need to change these two variables
PKG_NAME=stats_can
USER=ian.e.preston

OS=$TRAVIS_OS_NAME-64
mkdir ~/conda-bld
conda config --set anaconda_upload yes
export CONDA_BLD_PATH=~/conda-bld
export VERSION=`date +%Y.%m.%d`
conda build . --token $CONDA_UPLOAD_TOKEN --user $USER 