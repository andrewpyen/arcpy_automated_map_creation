# Docker Engine on Linux

This guide explains how to install and run Docker Engine on macOS **using Docker Engine**
---

## Prerequisites

- Linux OS
- There are no associated postgis/postgis linux/arm64/v8 manifests to use from Docker Hub, so we're using Linux amd machines
- Should use a vm with Linux or Windows

---

# Steps to Install Docker Engine and Run Docker Containers
Other things to make sure to install
* Curl
* VS Code
* Git
```bash
# Curl
sudo apt update
sudo apt install curl

# VS Code
# 1. Download Microsoft GPG key
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
sudo install -o root -g root -m 644 microsoft.gpg /etc/apt/trusted.gpg.d/
rm microsoft.gpg

# 2. Add VS Code repo
sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/repos/code stable main" > /etc/apt/sources.list.d/vscode.list'

# 3. Update and install
sudo apt update
sudo apt install code
```

* Install git
```bash
sudo apt update
sudo apt install git
```

1. Install Homebrew (if not already installed)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# If not already done, add Homebrew to you PATH:
# Run these commands in the terminal
echo >> /home/ubuntu/.bashrc
echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> /home/ubuntu/.bashrc
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
```

2. Install Docker and Docker Compose
```bash
brew install docker
brew install docker-compose

# Verify versions installed
docker version
docker-compose version
```

3. Install docker enginer and enable permissions
```bash
sudo apt install docker.io
newgrp docker
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

* If you need to change your sudo password, run the following commands:
```bash
sudo passwd your-username

# Then rerun the following
newgrp docker
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
```

---

5. Verify docker is running
```bash
docker version
docker run hello-world
```

---


# Start Running a Docker Container using `docker-compose.yaml`
* Run the container(s) in the FOREGROUND defined in `docker-runner/docker-compose.yaml`:
```bash
# Foreground
docker-compose up

# Background
docker-compose up -d
```

* Build the container from the build command (to start fresh) and start running the container
```bash
docker-compose up build
```

* Only build the container, but don't run it
```bash
docker-compose build
```

---

# Additional Resources
* [Colima Github](https://github.com/abiosoft/colima)
* [Docker CLI](https://docs.docker.com/engine/reference/commandline/cli/)
* [Docker Compose](https://docs.docker.com/compose/)