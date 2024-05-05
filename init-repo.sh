chmod +x ./run-with-docker.sh
chmod +x ./run-without-docker.sh
chmod +x ./local-test-script.sh

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    apt-get update -y
    apt-get install unzip
elif [[ "$OSTYPE" == "darwin"* ]]; then
  brew install p7zip
fi

