# Introduction 
TODO: Give a short introduction of your project. Let this section explain the objectives or the motivation behind this project. 

# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Authorizing using ssh git credentials using Git for Windows
1. Create a new key pair using the RSA algorithm:
```bash
ssh-keygen -t rsa -b 4096 -C "<your email>@heathus.com" -f ~/.ssh/id_rsa_heath

# Or if using ed25519 algorithm use:
# ssh-keygen -t ed25519 -C "<your email>@heathus.com" -f ~/.ssh/id_ed25519_heath

# Enter a password and remember this. You will need it each time you pull/push to the repo

# Print out the public key
cat ~/.ssh/id_rsa_heath.pub
```
Take this key and paste it into the Azure DevOps git settings under new key.

2. Create a ~/.ssh/config with this:
```bash

# if the ~/.ssh/config doesn't exist, then create and edit the file:
touch ~/.ssh/config
nano ~/.ssh/config

# Personal GitHub
## Only copy the github option if you ALSO have a personal github identity too
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_ed25519
  IdentitiesOnly yes

# Work Azure DevOps
Host heath-devops
  HostName ssh.dev.azure.com
  User git
  IdentityFile ~/.ssh/id_rsa_heath
  IdentitiesOnly yes
```

3. Clone the git repo using the custom label
```bash
# Before cloning the repo, make a new `repos` folder
cd ~/Documents
mkdir ~/repos
cd repos
git clone heath-devops:v3/HeathPBU/LSA-Web-Phase%20II/GIS-MAPCREATION

# After you confirm the repo, then change directories into it
cd GIS-MAPCREATION
```

4. Check feature brcatg anches or change to a new one
```bash
git status

# Create and check out a new branch
git checkout -b feature/survey-mapper-update
```

5. Configure your email and user name in Git
```bash
git config --global user.email "<your email>"
git config --global user.name "<your name>"
```

# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)