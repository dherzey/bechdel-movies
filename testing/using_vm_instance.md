## Connect to GCP VM instance locally
1. generate Windows public key
```bash
#create ssh key saved under .ssh folder
ssh-keygen -t rsa -f ~/.ssh/gcp -C username -b 2048

#get formatted public key
cat ~/.ssh/gcp.pub
```
2. add public key under SSH keys in VM instance
3. connect to VM instance locally (get externalIP of VM)
```bash
ssh -i ~/.ssh/gcp username@externalIP
```

## Install needed applications in VM for running of this project
```bash
#install Python
sudo apt-get install python

#install pip
sudo apt-get install pip

#install git
sudo apt-get install git

#install virtual environment
sudo apt-get install python3-venv

#clone this repo
git clone https://github.com/dherzey/bechdel-movies-project.git

#create virtual environment named project-venv
python3 -m venv project-venv

#activate virtual environment
source ./project-venv/bin/activate

#install all packages
sudo pip3 install -r requirements.txt
```