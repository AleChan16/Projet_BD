sudo curl -L https://dl.min.io/client/mc/release/linux-amd64/mc --create-dirs  -o $HOME/minio-binaries/mc
sudo chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/
mc --version
